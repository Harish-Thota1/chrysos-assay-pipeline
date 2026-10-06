"""
Reconciliation: did the pipeline catch everything?

Compares two independent records of the same events.

    the generator's ground truth          what actually changed in the source
    s3://.../change_log/*.json            written by the thing making the changes

    the pipeline's output                 what the pipeline managed to capture
    s3://.../bronze/<table>/snapshot/     the full load
    s3://.../bronze/<table>/delta/        every incremental run

The generator does not know the pipeline exists. The pipeline does not read
the generator's logs. So agreement between them is evidence, not circularity.


WHAT THIS MEASURES

MISSING   rows the generator inserted or updated inside the covered window
          that are not in S3. Must be zero. Anything else means the
          watermark or the look-back window is wrong.

STALE     rows sitting in S3 that have since been deleted in the source.
          Counted across the snapshot AND the deltas, because the snapshot
          is where most of them are. Nothing in an incremental pipeline can
          find these: no WHERE updated_at > x returns a row that is gone.
          This is the number delete handling exists to drive to zero, and
          the honest number to put in a README.


TWO BOUNDARY RULES, BOTH OF WHICH I GOT WRONG THE FIRST TIME

1. SCOPE BY WHAT THE DELTAS CLAIM TO COVER, not by all of history.
   Generator runs before the coverage window are in the snapshot. Runs
   after it have not been read yet. Comparing against everything invents
   hundreds of failures that are not failures.

2. THE BACKDATED ROW IS STAMPED BEFORE ITS RUN.
   Every generator run inserts one row deliberately stamped --lag-minutes
   in the past. So a run INSIDE the coverage window can contain a row
   whose updated_at is OUTSIDE it, at the start edge. That row was never
   in scope and counting it as lost blames the pipeline for the check's
   own arithmetic. Version one of this script reported exactly one
   missing row for exactly this reason.

A check that produces false failures is worse than no check, because you
stop trusting it and then you stop running it.


Run it locally:
    pip install boto3 pyarrow
    python3 reconcile.py --bucket <BUCKET>
"""

import argparse
import io
import json
import sys
from datetime import datetime, timedelta

import boto3
import pyarrow.parquet as pq


def iso(s):
    return datetime.fromisoformat(s)


def list_keys(s3, bucket, prefix, suffix):
    keys, token = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(suffix):
                keys.append(obj["Key"])
        if not resp.get("IsTruncated"):
            return sorted(keys)
        token = resp["NextContinuationToken"]


def read_ground_truth(s3, bucket, prefix):
    runs = []
    for key in list_keys(s3, bucket, prefix, ".json"):
        t = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())

        inserted = set(t.get("inserted_ids", []))
        # The duplicate_key defect inserts a real row whose id is recorded
        # under dirty_rows, not inserted_ids. Still a row to capture.
        for d in t.get("dirty_rows", []):
            if d.get("defect") == "duplicate_key" and d.get("measurement_id"):
                inserted.add(d["measurement_id"])

        back = t.get("backdated_row") or {}
        backdated_id = back.get("measurement_id")

        runs.append({
            "key":          key.split("/")[-1],
            "run_at":       iso(t["run_at"]),
            "inserted":     inserted,
            "backdated_id": backdated_id,
            "updated":      set(t.get("updated_recent_ids", []))
                            | set(t.get("updated_old_ids", [])),
            "deleted":      set(t.get("deleted_voided_ids", []))
                            | set(t.get("deleted_purged_ids", [])),
        })
    return runs


def read_ids(s3, bucket, prefix, want_windows=False):
    """measurement_ids out of every Parquet file under a prefix."""
    files = []
    for key in list_keys(s3, bucket, prefix, ".parquet"):
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        t = pq.read_table(io.BytesIO(body))
        entry = {
            "key":  key.split("/")[-1],
            "rows": t.num_rows,
            "ids":  set(t.column("measurement_id").to_pylist()),
        }
        if want_windows:
            cols = t.schema.names
            entry["window_start"] = (iso(t.column("_window_start")[0].as_py())
                                     if "_window_start" in cols else None)
            entry["window_end"] = (iso(t.column("_window_end")[0].as_py())
                                   if "_window_end" in cols else None)
        files.append(entry)
    return files


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--table", default="assay_measurement")
    p.add_argument("--prefix", default="bronze")
    p.add_argument("--change-log", dest="change_log", default="change_log/")
    p.add_argument("--lag-minutes", dest="lag_minutes", type=int, default=4,
                   help="the generator's deliberate backdating, must match "
                        "its --lag-minutes or the start edge is wrong")
    p.add_argument("--skip-snapshot", dest="skip_snapshot", action="store_true",
                   help="do not read the snapshot. faster, but the STALE "
                        "count then only covers the deltas and is far too low")
    p.add_argument("--show", type=int, default=10)
    a = p.parse_args(argv)

    s3 = boto3.client("s3")

    truth  = read_ground_truth(s3, a.bucket, a.change_log)
    deltas = read_ids(s3, a.bucket, f"{a.prefix}/{a.table}/delta/", want_windows=True)

    if not deltas:
        sys.exit(f"No delta files under {a.prefix}/{a.table}/delta/.")
    if not truth:
        sys.exit(f"No ground truth files under {a.change_log}.")

    snapshot = [] if a.skip_snapshot else read_ids(
        s3, a.bucket, f"{a.prefix}/{a.table}/snapshot/")

    windows = [(f["window_start"], f["window_end"]) for f in deltas
               if f["window_start"] and f["window_end"]]
    if not windows:
        sys.exit("Delta files carry no window metadata.")
    cover_start = min(w[0] for w in windows)
    cover_end   = max(w[1] for w in windows)

    delta_ids    = set().union(*(f["ids"] for f in deltas))
    snapshot_ids = set().union(*(f["ids"] for f in snapshot)) if snapshot else set()
    in_s3        = delta_ids | snapshot_ids

    deleted_any = set().union(*(r["deleted"] for r in truth)) if truth else set()

    # Boundary rule 2. A run inside the window can hold a backdated row whose
    # stamp is outside it. Exclude that one id when its stamp predates the
    # window, and count how often that happened so the exclusion is visible
    # rather than silent.
    lag = timedelta(minutes=a.lag_minutes)
    in_scope, excluded_backdated = [], []
    for r in truth:
        if not (cover_start <= r["run_at"] <= cover_end):
            continue
        inserted = set(r["inserted"])
        if r["backdated_id"]:
            if (r["run_at"] - lag) < cover_start:
                inserted.discard(r["backdated_id"])
                excluded_backdated.append((r["backdated_id"], r["key"]))
            else:
                inserted.add(r["backdated_id"])
        in_scope.append({**r, "inserted": inserted})

    expected_changed = set()
    for r in in_scope:
        expected_changed |= r["inserted"] | r["updated"]

    # A row inserted and deleted within the window can never be read by
    # anything, so it is not a loss.
    expected = expected_changed - deleted_any

    missing = expected - delta_ids
    stale   = in_s3 & deleted_any
    surplus = delta_ids - expected - deleted_any

    delta_rows = sum(f["rows"] for f in deltas)
    snap_rows  = sum(f["rows"] for f in snapshot)

    w = 60
    print("=" * w)
    print(f"  RECONCILIATION  ·  {a.table}")
    print("=" * w)
    print(f"  coverage start                {cover_start.isoformat()}")
    print(f"  coverage end                  {cover_end.isoformat()}")
    print()
    print(f"  generator runs, all time      {len(truth):>12,}")
    print(f"  generator runs in coverage    {len(in_scope):>12,}")
    print(f"  backdated rows excluded       {len(excluded_backdated):>12,}"
          f"   (stamped before the window opened)")
    print()
    print(f"  snapshot files / rows         {len(snapshot):>5,} / {snap_rows:>10,}"
          f"{'   SKIPPED' if a.skip_snapshot else ''}")
    print(f"  delta files / rows            {len(deltas):>5,} / {delta_rows:>10,}")
    print(f"  distinct ids in deltas        {len(delta_ids):>12,}")
    print(f"  duplicate rows from overlap   {delta_rows - len(delta_ids):>12,}"
          f"   (silver collapses these)")
    print()
    print("-" * w)
    print(f"  DID THE PIPELINE LOSE ANYTHING?")
    print(f"    expected in coverage        {len(expected):>12,}")
    print(f"    MISSING                     {len(missing):>12,}   <- must be 0")
    print(f"    surplus                     {len(surplus):>12,}")
    print("-" * w)
    print(f"  HOW BAD IS THE DELETE GAP?")
    print(f"    rows in S3                  {len(in_s3):>12,}")
    print(f"    deleted in the source       {len(deleted_any):>12,}")
    print(f"    STALE rows in S3            {len(stale):>12,}   <- deletes, not handled")
    if in_s3:
        print(f"    S3 that is wrong            {100.0 * len(stale) / len(in_s3):>11.2f}%")
    print("=" * w)

    if missing:
        print(f"\n  MISSING ids, first {a.show}:")
        for i in sorted(missing)[:a.show]:
            where = [r["key"] for r in in_scope
                     if i in r["inserted"] or i in r["updated"]]
            print(f"    {i}   recorded by {where[:2]}")
        print("\n  Before blaming the pipeline, check one of these rows in the")
        print("  source and compare its updated_at against the coverage start:")
        print("    psql -c \"select measurement_id, updated_at from "
              "assay_measurement where measurement_id = <id>\"")
        print("  A stamp before the coverage start means this check's boundary")
        print("  is wrong, not the pipeline. A stamp inside it means a real loss.")
    else:
        print("\n  Nothing lost. Every insert and update the generator recorded")
        print("  inside the covered window is present in S3.")

    if surplus:
        print(f"\n  Surplus, first {a.show}: {sorted(surplus)[:a.show]}")
        print("  Rows read that no in-coverage generator run claims. Usually")
        print("  rows from a run just outside the edge, picked up by the")
        print("  look-back, which is correct behaviour.")

    if a.skip_snapshot:
        print("\n  NOTE: snapshot skipped, so the STALE count covers only the")
        print("  deltas and is far too low. Most stale rows are in the snapshot.")

    print(f"\n  The stale count is the honest number. Those rows are in S3,")
    print(f"  deleted in the source, and no incremental read can ever find")
    print(f"  them, because a query cannot return a row that is not there.")

    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
