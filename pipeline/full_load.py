"""
Full load. Reads every row of every listed table and writes it to S3 as
Parquet, then records the time it started reading as the first watermark.

That watermark is the handover to incremental_load.py. Anything changed after
that instant is the incremental job's problem and this one has no opinion
about it.

WHY THE START TIME AND NOT THE FINISH TIME

This job takes a couple of minutes and rows change while it runs. A row
updated at second 50 might be read before or after its update, depending on
where the reader had got to. If the watermark were the finish time that row
would fall in a gap and nothing would ever look for it again. Taking the
start time means the incremental load reads it a second time, which is wasted
work, and wasted work is better than a row nobody will ever look for.

The watermark is written after every table is on disk, not during. A watermark
that moves before the data lands points past data nobody has.

Writes to <prefix>/<table>/snapshot/. The incremental job writes to
<prefix>/<table>/delta/ beside it, so a re-run of this job cannot wipe the
deltas.

Job parameters:
    --bucket       the S3 bucket to write into
    --prefix       path inside it
    --tables       comma separated
    --state-table  the DynamoDB control table
"""

import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

CONNECTION_NAME = "chrysos-source-connection"
DEFAULT_STATE_TABLE = "chrysos-pipeline-state"

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket", "prefix", "tables"])
bucket = args["bucket"]
prefix = args["prefix"]
tables = [t.strip() for t in args["tables"].split(",") if t.strip()]
state_table = args.get("state_table") or DEFAULT_STATE_TABLE

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------
# THE WATERMARK IS TAKEN HERE, before a single row is read.
# ---------------------------------------------------------------------------
read_start = datetime.now(timezone.utc)

print(f"full load read start  {read_start.isoformat()}")
print(f"target                s3://{bucket}/{prefix}/<table>/snapshot/")
print(f"control table         {state_table}")

summary = []

for table in tables:
    short = table.split(".")[-1]
    target = f"s3://{bucket}/{prefix}/{short}/snapshot/"

    print(f"\n--- {table} ---")

    # Reading through the CONNECTION, not a hardcoded URL. The hostname,
    # username and password live in the connection, so nothing secret is
    # in this file and it is safe to commit.
    frame = glue_context.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": CONNECTION_NAME,
            "dbtable": table,
        },
        transformation_ctx=f"read_{short}",
    )

    df = frame.toDF()
    rows = df.count()
    print(f"read {rows:,} rows, {len(df.columns)} columns")
    df.printSchema()

    # overwrite, because a snapshot replaces rather than adds to.
    # This only ever touches .../snapshot/, never .../delta/.
    df.write.mode("overwrite").parquet(target)
    print(f"wrote {rows:,} rows to {target}")

    summary.append((short, rows))

# ---------------------------------------------------------------------------
# Every table is on disk. Only now is it safe to move the watermark.
# ---------------------------------------------------------------------------
ddb = boto3.client("dynamodb")
written_at = datetime.now(timezone.utc)

for short, rows in summary:
    ddb.put_item(
        TableName=state_table,
        Item={
            "table_name":    {"S": short},
            "watermark":     {"S": read_start.isoformat()},
            "set_by":        {"S": "full_load"},
            "set_at":        {"S": written_at.isoformat()},
            "rows_at_set":   {"N": str(rows)},
            "job_run_id":    {"S": args.get("JOB_RUN_ID", "unknown")},
        },
    )
    print(f"watermark for {short} set to {read_start.isoformat()}")

elapsed = (written_at - read_start).total_seconds()
print("\n" + "=" * 54)
for name, rows in summary:
    print(f"  {name:<24} {rows:>12,} rows")
print(f"  {'watermark':<24} {read_start.isoformat():>12}")
print(f"  {'elapsed':<24} {elapsed:>12.1f} s")
print("=" * 54)

job.commit()
