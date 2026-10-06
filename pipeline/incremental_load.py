"""
Incremental load. Reads only the rows that changed since the last successful
run and appends them to S3 as Parquet.

A Python Shell job, not Spark: 180 rows does not need a cluster, and
0.0625 DPU costs about a thirtieth of 2 DPU.

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


THREE SAFEGUARDS, for the cases the five decisions above do not cover.

A. THE READ IS BATCHED, through a server-side cursor.
   180 rows fit in memory. A six hour backlog after an outage might not.
   A named cursor leaves the rows on the server and hands over
   --batch-rows at a time, and each batch is written as its own file.
   An ordinary cursor would not help: it sends the whole result to the
   client on execute, so the rows are already here before any fetch.
   If batch 4 of 9 fails, batches 1 to 3 are in S3 and the watermark has
   not moved, so the next run reads the window again and writes those
   rows twice. Duplicates are what silver already removes. Half a window
   with a moved watermark would be a hole.
   The cost: the read transaction stays open while the files are written,
   so a long backlog holds a transaction open on the source and vacuum
   cannot clean up behind it. At a normal 180 rows that is a second. If
   backlogs became routine, the batches would be staged to local disk
   first and uploaded after the connection closed.

B. THE WATERMARK WRITE IS CONDITIONAL on the value this run read.
   If another run has moved it in the meantime, this write fails rather
   than overwriting it, and the job exits saying so. This is the reason
   the control table is DynamoDB and not a file in S3: S3 cannot do a
   conditional write on a value, so two overlapping runs would both
   succeed and the later one would win, silently.

C. THE COLUMN LIST IS STORED AND COMPARED each run.
   A column added in Postgres would otherwise never appear in S3 and
   nothing would mention it. The check does not fail the run, because a
   harmless added column should not stop ingestion. It prints the added
   and removed names and records them in the run log.


Job parameters:
    --pghost --pgport --pgdatabase --pguser --pgpassword
    --s3-bucket          bucket to write into
    --s3-prefix          path inside it                 (default: bronze)
    --source-table       table to read                  (default: assay_measurement)
    --state-table        DynamoDB control table         (default: chrysos-pipeline-state)
    --lookback-minutes   deliberate overlap             (default: 15)
    --max-window-hours   safety valve on a big backlog  (default: 6)
    --batch-rows         rows per file, and per fetch   (default: 200,000)
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
    p.add_argument("--batch-rows", dest="batch_rows", type=int, default=200_000)
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
    raw = item["watermark"]["S"]
    columns = item.get("columns", {}).get("S") or ""
    # The raw string goes back out as the compare-and-set condition, so it is
    # kept exactly as stored. A parsed-then-reformatted timestamp can differ
    # by a trailing zero and the condition would never match.
    return datetime.fromisoformat(raw), raw, columns


def fetch_batches(conn, table, window_start, window_end, batch_rows):
    """Stream the window out of Postgres, batch_rows at a time.

    The upper bound matters. Without it, the rows read would be 'everything
    up to whenever each row happened to be scanned', which is not a window
    at all and cannot be recorded as one. With it, the run can say exactly
    which interval it covered, and the watermark it saves is the interval's
    end rather than a guess.

    A NAMED cursor, which means server side. An ordinary cursor sends the
    whole result to the client on execute, so fetching in batches would do
    nothing for memory: the rows are already here. A named cursor leaves them
    on the server and hands over batch_rows at a time.

    A named cursor needs a transaction, which is why the caller does not set
    autocommit.

    One run per batch would be wrong, so this yields batches and the caller
    writes one file each. If batch 4 of 9 fails, batches 1 to 3 are already in
    S3 and the watermark has not moved, so the next run reads the whole window
    again and writes those rows a second time. Duplicates are what silver
    already removes. Half a window and a moved watermark would be a hole.
    """
    with conn.cursor(name="incr_read",
                     cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.itersize = batch_rows
        cur.execute(
            f"SELECT * FROM {table} "
            f" WHERE updated_at >  %s "
            f"   AND updated_at <= %s "
            f" ORDER BY updated_at, measurement_id",
            (window_start, window_end),
        )
        while True:
            rows = cur.fetchmany(batch_rows)
            if not rows:
                return
            # description is None on a named cursor until the first fetch.
            yield rows, cur.description


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
    watermark, watermark_raw, prev_columns = read_watermark(
        ddb, args.state_table, short)

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

    # 4. read and write in batches, data FIRST
    #
    # The file name carries the run id and a part number. window_end alone is
    # not unique: two runs started in the same second would write to the same
    # key and the second would overwrite the first. MaxConcurrentRuns = 1
    # prevents that today, but that is a setting on the job, not a property of
    # this code, and settings get changed.
    stamp = window_end.strftime("%Y%m%dT%H%M%SZ")
    run_tag = args.job_run_id[-8:] if args.job_run_id != "local" else "local"

    meta = {
        "_ingested_at":    read_start.isoformat(),
        "_window_start":   window_start.isoformat(),
        "_window_end":     window_end.isoformat(),
        "_job_run_id":     args.job_run_id,
    }

    total_rows = 0
    written_bytes = 0
    parts = []
    oldest = newest = None
    ids = set()
    columns_seen = ""

    conn = connect(args)
    conn.set_session(readonly=True)
    try:
        for part_no, (rows, description) in enumerate(
                fetch_batches(conn, args.source_table, window_start,
                              window_end, args.batch_rows)):
            total_rows += len(rows)
            stamps = [r["updated_at"] for r in rows]
            oldest = min(stamps) if oldest is None else min(oldest, min(stamps))
            newest = max(stamps) if newest is None else max(newest, max(stamps))
            ids.update(r["measurement_id"] for r in rows)
            if not columns_seen:
                columns_seen = ",".join(c.name for c in description)

            if args.dry_run:
                continue

            key = (f"{args.s3_prefix}/{short}/delta/"
                   f"incr_{stamp}_{run_tag}_p{part_no:03d}.parquet")
            arrow_table = to_arrow(rows, description, meta)
            n = write_parquet(s3, arrow_table, args.s3_bucket, key)
            written_bytes += n
            parts.append(key)
            print(f"wrote            s3://{args.s3_bucket}/{key}  ({n:,} bytes)")
    finally:
        conn.close()

    print(f"rows read        {total_rows:,}")
    if total_rows:
        print(f"oldest change    {oldest.isoformat()}")
        print(f"newest change    {newest.isoformat()}")
        print(f"distinct ids     {len(ids):,}   (equals rows read, "
              f"because measurement_id is the primary key)")
        print(f"files written    {len(parts)}   "
              f"(batch size {args.batch_rows:,})")
    else:
        # No file for an empty window. An empty Parquet file is a file
        # someone has to open to discover it is empty.
        print("wrote            nothing, no rows changed in this window")

    # 5. did the source change shape?
    #
    # A new column in Postgres would otherwise never appear in S3 and nothing
    # would say so. This does not fail the run: a harmless added column should
    # not stop ingestion. It makes the change loud and records it, so the drift
    # is discovered now rather than when someone asks where the column went.
    drift = None
    if columns_seen and prev_columns and columns_seen != prev_columns:
        before = set(prev_columns.split(","))
        after = set(columns_seen.split(","))
        drift = {"added": sorted(after - before),
                 "removed": sorted(before - after)}
        print(f"\nSCHEMA CHANGED since the last run")
        print(f"  added            {drift['added'] or 'none'}")
        print(f"  removed          {drift['removed'] or 'none'}")
        print(f"  added columns are in this run's files. removed ones stop")
        print(f"  appearing, and older files still carry them.\n")

    if args.dry_run:
        would_write = -(-total_rows // args.batch_rows)   # ceiling division
        print("\nDRY RUN. nothing written, watermark not moved.")
        print(f"would have written  {would_write} file(s) under "
              f"s3://{args.s3_bucket}/{args.s3_prefix}/{short}/delta/")
        return 0

    # 6. and only now move the watermark, and only if nobody else moved it
    #
    # Compare-and-set. If another run has written a watermark since this run
    # read it, this write fails instead of overwriting it. Picking DynamoDB
    # over a file in S3 was for exactly this: S3 cannot do a conditional
    # write on a value, so two overlapping runs there would both succeed and
    # the later one would win silently.
    try:
        ddb.put_item(
            TableName=args.state_table,
            Item={
                "table_name":    {"S": short},
                "watermark":     {"S": window_end.isoformat()},
                "columns":       {"S": columns_seen or prev_columns},
                "set_by":        {"S": "incremental_load"},
                "set_at":        {"S": datetime.now(timezone.utc).isoformat()},
                "rows_at_set":   {"N": str(total_rows)},
                "job_run_id":    {"S": args.job_run_id},
            },
            ConditionExpression="watermark = :seen",
            ExpressionAttributeValues={":seen": {"S": watermark_raw}},
        )
    except ddb.exceptions.ConditionalCheckFailedException:
        sys.exit(
            f"\nWatermark moved while this run was working.\n"
            f"  read       {watermark_raw}\n"
            f"  tried      {window_end.isoformat()}\n"
            f"Another run is active. This run's files are already in S3 and\n"
            f"will be de-duplicated in silver. The watermark was NOT moved,\n"
            f"so nothing has been skipped. Check MaxConcurrentRuns = 1."
        )
    print(f"watermark out    {window_end.isoformat()}")

    # 7. a run log, so the history of windows is auditable without DynamoDB
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
        "batch_rows":     args.batch_rows,
        "rows_read":      total_rows,
        "bytes_written":  written_bytes,
        "s3_keys":        parts,
        "schema_drift":   drift,
        "finished_at":    datetime.now(timezone.utc).isoformat(),
    }
    s3.put_object(
        Bucket=args.s3_bucket,
        Key=f"_runlog/incremental/{short}/{stamp}_{run_tag}.json",
        Body=json.dumps(log, indent=2).encode(),
    )

    elapsed = (datetime.now(timezone.utc) - read_start).total_seconds()
    print(f"elapsed          {elapsed:.1f} s")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
