"""
Incremental load. Reads only the rows that changed since the last
successful run and appends them to S3 as Parquet.

Reads only the rows that changed since the last successful run and appends
them to S3 as Parquet. A Python Shell job, not Spark: 180 rows does not
need a cluster, and 0.0625 DPU costs about a thirtieth of 2 DPU.

                watermark (DynamoDB)
                        |
   Postgres  ---->  this job  ---->  s3://<bucket>/<prefix>/<table>/delta/
                        |
                        +---->  watermark moved forward, after the write


THE FIVE DECISIONS IN HERE, and why each one is the way it is.

1. THE WATERMARK IS THE READ START TIME, never the finish time.
   The read takes a few seconds. Rows change during those seconds. A row
   changed at second 2 may be read before or after its change depending
   on where the reader had got to. Taking the start time means that row
   gets read again next run. Taking the finish time means it falls in a
   gap and nothing ever looks for it again.
   Re-reading is work you can see. A gap is silent and permanent.

2. THE LOOK-BACK WINDOW IS MEASURED FROM THE WATERMARK, not from now.
       updated_at > watermark - lookback      correct
       updated_at > now()     - lookback      silently loses a backlog
   If a run fails and the next one is an hour late, the first version
   reads the whole hour plus the overlap. The second reads the last few
   minutes and skips the rest without mentioning it.

3. THE LOOK-BACK EXISTS BECAUSE A ROW'S STAMP CAN PREDATE ITS ARRIVAL.
   PostgreSQL's now() is frozen at the start of the transaction, so a
   transaction that begins at 11:10 and commits at 11:16 leaves a row
   stamped 11:10 that nobody could see until 11:16. A run at 11:15
   cannot see it. A run at 11:30 reading from 11:15 would skip it,
   because 11:10 is behind 11:15. Reading from 11:00 catches it.
   clock_timestamp() would not help: the problem is that NO timestamp
   taken at write time can equal the commit time, because at write time
   the transaction does not yet know when it will commit.

4. WRITES ARE APPEND, NEVER OVERWRITE.
   There is no "update a row" in S3. Parquet files cannot be edited.
   An overwrite would delete every file under the prefix and replace it
   with this run's handful of rows. So each run writes its own new file
   and the current value of a row is worked out at READ time, in silver,
   by taking the newest version of each measurement_id.
   Bronze is a log of what was seen, not a picture of what is true now.

5. THE WATERMARK MOVES AFTER THE WRITE SUCCEEDS, never before.
   If the watermark moved first and the write then failed, the next run
   would start after rows that never landed. Moving it last means a
   failed run is simply repeated. The watermark records what was
   successfully WRITTEN, not what was successfully read.


AND ONE THING THAT IS DELIBERATELY NOT DONE HERE.

No de-duplication. Within a single run there is nothing to de-duplicate:
measurement_id is the primary key, so each row comes back exactly once.
The duplicates come from the overlap BETWEEN runs, which means they can
only be resolved by looking at more than one run's output, which means
they belong in silver at read time:

    row_number() over (partition by measurement_id order by updated_at desc) = 1

Doing it here would be doing nothing while looking like doing something.


Job parameters:
    --pghost --pgport --pgdatabase --pguser --pgpassword
    --s3-bucket          bucket to write into
    --s3-prefix          path inside it                 (default: bronze)
    --source-table       table to read                  (default: assay_measurement)
    --state-table        DynamoDB control table         (default: chrysos-pipeline-state)
    --lookback-minutes   deliberate overlap             (default: 15)
    --max-window-hours   safety valve on a big backlog  (default: 6)
    --dry-run            read and report, write nothing, do not move the watermark
"""

import argparse
import io
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_STATE_TABLE = "chrysos-pipeline-state"
SIX_PLACES = Decimal("0.000001")

# PostgreSQL type OID -> pyarrow type.
#
# This map exists so that a column which happens to be entirely NULL in one
# run does not get written as pyarrow's "null" type in that run's file and as
# a real type in the next one. Two files under the same prefix with different
# types for the same column is a read-time failure that shows up days later
# and looks like data corruption. crm_code is NULL for every row that is not
# a CRM sample, so this is not hypothetical.
OID_TO_ARROW = {
    16:   pa.bool_(),                              # bool
    20:   pa.int64(),                              # int8
    21:   pa.int16(),                              # int2
    23:   pa.int32(),                              # int4
    25:   pa.string(),                             # text
    700:  pa.float32(),                            # float4
    701:  pa.float64(),                            # float8
    1043: pa.string(),                             # varchar
    1114: pa.timestamp("us"),                      # timestamp
    1184: pa.timestamp("us", tz="UTC"),            # timestamptz
    1700: pa.decimal128(20, 6),                    # numeric
}


def parse_args(argv):
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--pghost")
    p.add_argument("--pgport", default="5432")
    p.add_argument("--pgdatabase", default="chrysos")
    p.add_argument("--pguser", default="postgres")
    p.add_argument("--pgpassword")
    p.add_argument("--s3-bucket", dest="s3_bucket")
    p.add_argument("--s3-prefix", dest="s3_prefix", default="bronze")
    p.add_argument("--source-table", dest="source_table", default="assay_measurement")
    p.add_argument("--state-table", dest="state_table", default=DEFAULT_STATE_TABLE)
    p.add_argument("--lookback-minutes", dest="lookback_minutes", type=int, default=15)
    p.add_argument("--max-window-hours", dest="max_window_hours", type=int, default=6)
    p.add_argument("--dry-run", dest="dry_run", action="store_true")
    p.add_argument("--JOB_NAME", dest="job_name", default="local")
    p.add_argument("--JOB_RUN_ID", dest="job_run_id", default="local")
    # Glue injects parameters this script does not care about, so unknown
    # arguments are ignored rather than fatal.
    known, _ = p.parse_known_args(argv)
    return known


def connect(args):
    """Explicit parameters, or the PG* environment variables for local runs.

    Deliberately NOT boto3 glue.get_connection(). The Glue API lives on the
    public internet, and a Glue job attached to a VPC has no route there, so
    that call hangs until the job times out. Learned the hard way in 2.5.
    """
    if args.pghost:
        return psycopg2.connect(
            host=args.pghost, port=int(args.pgport), dbname=args.pgdatabase,
            user=args.pguser, password=args.pgpassword)
    if not os.environ.get("PGHOST"):
        sys.exit("No connection details. Pass --pghost ... , or run 'source db.env'.")
    return psycopg2.connect("")


def read_watermark(ddb, state_table, table_name):
    """Fetch the watermark, or stop.

    Stopping is deliberate. The alternatives are both worse:
      - default to 1970, and the first run quietly does a full load with
        the wrong tool, taking minutes and writing 215,000 rows into the
        delta prefix where they do not belong
      - default to now, and everything before this moment is skipped for
        good, silently
    An absent watermark means the handover from the full load did not
    happen. That is a setup problem and it should be loud.
    """
    got = ddb.get_item(
        TableName=state_table,
        Key={"table_name": {"S": table_name}},
        ConsistentRead=True,
    )
    item = got.get("Item")
    if not item:
        sys.exit(
            f"No watermark for '{table_name}' in {state_table}.\n"
            f"The full load seeds it. Run full_load.py first, or set it by hand:\n"
            f"  aws dynamodb put-item --table-name {state_table} --item "
            f"'{{\"table_name\":{{\"S\":\"{table_name}\"}},"
            f"\"watermark\":{{\"S\":\"2026-10-02T00:00:00+00:00\"}}}}'"
        )
    return datetime.fromisoformat(item["watermark"]["S"])


def fetch_changes(conn, table, window_start, window_end):
    """Read the rows whose updated_at falls in the window.

    The upper bound matters. Without it, the rows read would be 'everything
    up to whenever each row happened to be scanned', which is not a window
    at all and cannot be recorded as one. With it, the run can say exactly
    which interval it covered, and the watermark it saves is the interval's
    end rather than a guess.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            f"SELECT * FROM {table} "
            f" WHERE updated_at >  %s "
            f"   AND updated_at <= %s "
            f" ORDER BY updated_at, measurement_id",
            (window_start, window_end),
        )
        rows = cur.fetchall()
        description = cur.description
    return rows, description


def to_arrow(rows, description, meta):
    """Build a Parquet table with an EXPLICIT schema from the cursor's OIDs.

    meta columns are lineage, not data. They let anyone reading bronze later
    answer 'which run wrote this row, and what window did that run claim to
    cover', without reading the run logs.
    """
    columns, fields = {}, []

    for idx, col in enumerate(description):
        arrow_type = OID_TO_ARROW.get(col.type_code)
        if arrow_type is None:
            raise RuntimeError(
                f"column '{col.name}' has unmapped PostgreSQL OID {col.type_code}. "
                f"Add it to OID_TO_ARROW rather than letting pyarrow guess."
            )
        values = [r[idx] for r in rows]
        if arrow_type == pa.decimal128(20, 6):
            values = [None if v is None else Decimal(v).quantize(SIX_PLACES)
                      for v in values]
        columns[col.name] = pa.array(values, type=arrow_type)
        fields.append(pa.field(col.name, arrow_type))

    n = len(rows)
    for name, value in meta.items():
        columns[name] = pa.array([value] * n, type=pa.string())
        fields.append(pa.field(name, pa.string()))

    return pa.Table.from_arrays(list(columns.values()), schema=pa.schema(fields))


def write_parquet(s3, table, bucket, key):
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    body = buf.getvalue()
    s3.put_object(Bucket=bucket, Key=key, Body=body)
    return len(body)


def main(argv):
    args = parse_args(argv)
    short = args.source_table.split(".")[-1]

    ddb = boto3.client("dynamodb")
    s3 = boto3.client("s3")

    # 1. where did we get to last time
    watermark = read_watermark(ddb, args.state_table, short)

    # 2. the new watermark, taken BEFORE the read
    read_start = datetime.now(timezone.utc)

    # 3. the window: deliberate overlap at the start, capped at the end
    window_start = watermark - timedelta(minutes=args.lookback_minutes)
    window_end = read_start
    capped = False
    if (window_end - window_start) > timedelta(hours=args.max_window_hours):
        # A long outage would otherwise return a backlog too big for 1 GB of
        # RAM. Covering part of it and letting the next run continue is
        # slower to catch up and cannot fall over.
        window_end = window_start + timedelta(hours=args.max_window_hours)
        capped = True

    print(f"table            {args.source_table}")
    print(f"watermark in     {watermark.isoformat()}")
    print(f"lookback         {args.lookback_minutes} min")
    print(f"window           {window_start.isoformat()}  ->  {window_end.isoformat()}")
    print(f"window span      {(window_end - window_start).total_seconds() / 60:.1f} min"
          f"{'   CAPPED, a backlog is being worked through' if capped else ''}")

    conn = connect(args)
    conn.set_session(readonly=True, autocommit=True)
    try:
        rows, description = fetch_changes(conn, args.source_table,
                                          window_start, window_end)
    finally:
        conn.close()

    print(f"rows read        {len(rows):,}")

    if rows:
        stamps = [r["updated_at"] for r in rows]
        print(f"oldest change    {min(stamps).isoformat()}")
        print(f"newest change    {max(stamps).isoformat()}")
        ids = {r["measurement_id"] for r in rows}
        print(f"distinct ids     {len(ids):,}   (equals rows read, "
              f"because measurement_id is the primary key)")

    stamp = window_end.strftime("%Y%m%dT%H%M%SZ")
    key = f"{args.s3_prefix}/{short}/delta/incr_{stamp}.parquet"

    if args.dry_run:
        print("\nDRY RUN. nothing written, watermark not moved.")
        print(f"would have written  s3://{args.s3_bucket}/{key}")
        return 0

    # 4. write the data FIRST
    written_bytes = 0
    if rows:
        meta = {
            "_ingested_at":    read_start.isoformat(),
            "_window_start":   window_start.isoformat(),
            "_window_end":     window_end.isoformat(),
            "_job_run_id":     args.job_run_id,
        }
        arrow_table = to_arrow(rows, description, meta)
        written_bytes = write_parquet(s3, arrow_table, args.s3_bucket, key)
        print(f"wrote            s3://{args.s3_bucket}/{key}  ({written_bytes:,} bytes)")
    else:
        # No file for an empty window. An empty Parquet file is a file
        # someone has to open to discover it is empty.
        print("wrote            nothing, no rows changed in this window")

    # 5. and only now move the watermark
    ddb.put_item(
        TableName=args.state_table,
        Item={
            "table_name":    {"S": short},
            "watermark":     {"S": window_end.isoformat()},
            "set_by":        {"S": "incremental_load"},
            "set_at":        {"S": datetime.now(timezone.utc).isoformat()},
            "rows_at_set":   {"N": str(len(rows))},
            "job_run_id":    {"S": args.job_run_id},
        },
    )
    print(f"watermark out    {window_end.isoformat()}")

    # 6. a run log, so the history of windows is auditable without DynamoDB
    log = {
        "table":          short,
        "job_run_id":     args.job_run_id,
        "read_start":     read_start.isoformat(),
        "watermark_in":   watermark.isoformat(),
        "watermark_out":  window_end.isoformat(),
        "window_start":   window_start.isoformat(),
        "window_end":     window_end.isoformat(),
        "window_capped":  capped,
        "lookback_min":   args.lookback_minutes,
        "rows_read":      len(rows),
        "bytes_written":  written_bytes,
        "s3_key":         key if rows else None,
        "finished_at":    datetime.now(timezone.utc).isoformat(),
    }
    s3.put_object(
        Bucket=args.s3_bucket,
        Key=f"_runlog/incremental/{short}/{stamp}.json",
        Body=json.dumps(log, indent=2).encode(),
    )

    elapsed = (datetime.now(timezone.utc) - read_start).total_seconds()
    print(f"elapsed          {elapsed:.1f} s")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
