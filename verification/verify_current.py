"""
Is S3 current, not just complete?

reconcile_v2.py answers "did every changed row arrive". This answers the
harder question: "does S3 hold the same version of each row that the source
holds right now".

Those are different claims and the gap between them is real. A row can
arrive and then go stale, and no id-only comparison will ever notice.

    S3       measurement_id  ->  MAX(updated_at) across snapshot and deltas
    source   measurement_id  ->  updated_at

Four outcomes per row:

    CURRENT   both sides agree
    BEHIND    S3 has an older version than the source
    MISSING   the source has it, S3 does not
    STALE     S3 has it, the source does not. deleted.


THE BOUNDARY RULE, which is the whole reason this check is trustworthy

A row changed AFTER the last watermark is not due yet. The next incremental
run will collect it. Counting it as BEHIND or MISSING would report a failure
every single time this script runs, and a check that always fails is a check
nobody reads.

So the watermark is read from DynamoDB and the rows are split:

    source updated_at <= watermark     S3 is responsible. must be CURRENT.
    source updated_at >  watermark     not due yet. reported separately.

STALE ignores the watermark. A row deleted from the source is stale in S3
whenever it was deleted, because nothing will ever come back for it.


Run it locally:
    pip install boto3 pyarrow psycopg2-binary
    source db.env
    python3 verify_current.py --bucket <BUCKET>
"""

import argparse
import io
import os
import sys
from datetime import datetime, timezone

import boto3
import psycopg2
import pyarrow.parquet as pq


def to_utc(dt, assume=timezone.utc):
    """Normalise every timestamp to one timezone-aware form.

    WHY THIS EXISTS. The snapshot and the delta files disagree about this
    column. Spark read PostgreSQL timestamptz and wrote plain `timestamp`,
    dropping the zone. incremental_load.py writes `timestamp[us, tz=UTC]`.
    Comparing one against the other raises TypeError, which is Python
    refusing to guess. Good. A silent guess here would mean comparing
    instants 9.5 hours apart and calling rows stale that are fine.

    The naive values ARE UTC instants: Glue's Spark session timezone is UTC,
    so the zone was dropped rather than shifted.

    And that assumption tests itself. If it were wrong, every snapshot row
    would look 9.5 hours out and BEHIND would come back in the hundreds of
    thousands rather than at zero. A wrong assumption here cannot hide.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=assume)
    return dt.astimezone(timezone.utc)


def list_keys(s3, bucket, prefix):
    keys, token = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in r.get("Contents", []) if o["Key"].endswith(".parquet")]
        if not r.get("IsTruncated"):
            return sorted(keys)
        token = r["NextContinuationToken"]


def s3_latest_versions(s3, bucket, prefixes):
    """measurement_id -> the newest updated_at anywhere in S3.

    This is exactly what silver would compute with
        row_number() over (partition by measurement_id order by updated_at desc)
    done here in Python so the check does not depend on silver existing.
    """
    latest, files, rows = {}, 0, 0
    for prefix in prefixes:
        for key in list_keys(s3, bucket, prefix):
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            t = pq.read_table(io.BytesIO(body),
                             columns=["measurement_id", "updated_at"])
            ids = t.column("measurement_id").to_pylist()
            ups = [to_utc(u) for u in t.column("updated_at").to_pylist()]
            for i, u in zip(ids, ups):
                if u is None:
                    continue
                if i not in latest or u > latest[i]:
                    latest[i] = u
            files += 1
            rows += t.num_rows
    return latest, files, rows


def source_versions(table):
    if not os.environ.get("PGHOST"):
        sys.exit("PGHOST is not set. Run 'source db.env' first.")
    conn = psycopg2.connect("")
    # readonly, but NOT autocommit. A named (server-side) cursor only exists
    # inside a transaction, so autocommit=True leaves it nothing to live in
    # and psycopg2 refuses. Those two settings cannot both be on.
    conn.set_session(readonly=True)
    try:
        with conn.cursor(name="stream") as cur:   # server-side cursor, no 217k rows in RAM at once
            cur.itersize = 20000
            cur.execute(f"SELECT measurement_id, updated_at FROM {table}")
            return {i: to_utc(u) for i, u in cur}
    finally:
        conn.close()


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--table", default="assay_measurement")
    p.add_argument("--prefix", default="bronze")
    p.add_argument("--state-table", dest="state_table", default="chrysos-pipeline-state")
    p.add_argument("--show", type=int, default=5)
    a = p.parse_args(argv)

    s3  = boto3.client("s3")
    ddb = boto3.client("dynamodb")

    item = ddb.get_item(TableName=a.state_table,
                        Key={"table_name": {"S": a.table}},
                        ConsistentRead=True).get("Item")
    if not item:
        sys.exit(f"No watermark for {a.table}. Run the full load first.")
    watermark = to_utc(datetime.fromisoformat(item["watermark"]["S"]))

    print("reading S3 ...")
    in_s3, files, rows = s3_latest_versions(
        s3, a.bucket,
        [f"{a.prefix}/{a.table}/snapshot/", f"{a.prefix}/{a.table}/delta/"])

    print("reading the source ...")
    in_src = source_versions(a.table)

    current, behind, missing, not_due = [], [], [], []
    for i, src_u in in_src.items():
        s3_u = in_s3.get(i)
        if src_u > watermark:
            not_due.append(i)            # the next run's job, not a fault
        elif s3_u is None:
            missing.append(i)
        elif s3_u < src_u:
            behind.append((i, s3_u, src_u))
        else:
            current.append(i)

    stale = [i for i in in_s3 if i not in in_src]

    # S3 ahead of the source should be impossible. If it happens, either the
    # source went backwards or something wrote fabricated timestamps.
    impossible = [i for i, u in in_s3.items()
                  if i in in_src and u > in_src[i]]

    due = len(current) + len(behind) + len(missing)
    w = 60
    print("\n" + "=" * w)
    print(f"  IS S3 CURRENT?  ·  {a.table}")
    print("=" * w)
    print(f"  watermark                     {watermark.isoformat()}")
    print(f"  S3 files / rows read          {files:>5,} / {rows:>10,}")
    print(f"  distinct ids in S3            {len(in_s3):>12,}")
    print(f"  rows in the source            {len(in_src):>12,}")
    print("-" * w)
    print(f"  ROWS S3 IS RESPONSIBLE FOR    {due:>12,}   (updated_at <= watermark)")
    print(f"    CURRENT                     {len(current):>12,}")
    print(f"    BEHIND                      {len(behind):>12,}   <- must be 0")
    print(f"    MISSING                     {len(missing):>12,}   <- must be 0")
    if due:
        print(f"    correct                     {100.0 * len(current) / due:>11.3f}%")
    print("-" * w)
    print(f"  NOT DUE YET                   {len(not_due):>12,}   (changed after the watermark)")
    print("-" * w)
    print(f"  STALE in S3                   {len(stale):>12,}   <- deleted in the source")
    if in_s3:
        print(f"  S3 that is wrong              {100.0 * len(stale) / len(in_s3):>11.3f}%")
    if impossible:
        print(f"  IMPOSSIBLE (S3 ahead)         {len(impossible):>12,}   <- investigate")
    print("=" * w)

    if behind:
        print(f"\n  BEHIND, first {a.show}:")
        for i, s3_u, src_u in sorted(behind)[:a.show]:
            print(f"    {i}   S3 {s3_u.isoformat()}   source {src_u.isoformat()}")
        print("  S3 holds an older version of these rows than the source does,")
        print("  and they changed before the watermark, so a run should have")
        print("  collected them. Check the look-back width first.")

    if missing:
        print(f"\n  MISSING, first {a.show}: {sorted(missing)[:a.show]}")
        print("  In the source, changed before the watermark, absent from S3.")
        print("  This is the serious one. Rows nothing will come back for.")

    if not behind and not missing:
        print("\n  Every row S3 is responsible for is present and current.")
        print("  Not 'nothing is missing', which id counting can show, but")
        print("  'every version matches', which it cannot.")

    if stale:
        print(f"\n  Stale examples: {sorted(stale)[:a.show]}")
        print("  These exist in S3 and not in the source. No incremental read")
        print("  will ever find them, because a query cannot return a row that")
        print("  is gone. Stage 4 reconciles keys to detect exactly these.")

    return 1 if (behind or missing or impossible) else 0


if __name__ == "__main__":
    sys.exit(main())
