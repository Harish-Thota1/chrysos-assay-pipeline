# pipeline/

The two jobs that move data from Postgres into S3.

```
full_load.py          runs once       Spark, 2 x G.1X
incremental_load.py   every 15 min    Python Shell, 0.0625 DPU
```

Both run as AWS Glue jobs. Glue reads the code from S3, not from a laptop, so
editing a file here means uploading it again:

```
aws s3 cp pipeline/incremental_load.py s3://<BUCKET>/glue-scripts/
```

---

## full_load.py

Copies every row of the table to S3, once, then saves the first watermark.

```
reads    assay_measurement and machine, all rows
writes   s3://<bucket>/bronze/<table>/snapshot/
then     saves the time it started reading, as the first watermark
```

It runs once, at setup. Everything after it is the incremental job's work.

### Why it saves the start time, not the finish time

This job takes about two minutes. Rows change while it runs.

Say a row changes 50 seconds in. Whether the job read that row before or after
the change depends on where the reader had got to, which is luck.

```
save the finish time    the incremental job starts after it.
                        the changed row is never read again.

save the start time     the incremental job starts before it.
                        the changed row is read a second time.
```

Reading a row twice is wasted work you can see. Missing one is silent and
permanent.

### Why it writes the watermark last

The watermark is written after every table is on disk, not during. A watermark
that moves before the data lands points past data nobody has.

If the job crashes halfway, there is no watermark at all, and the incremental
job refuses to start rather than starting from a point the snapshot never
reached.

### Why snapshot and delta are separate folders

```
bronze/assay_measurement/snapshot/     this job writes here
bronze/assay_measurement/delta/        the incremental job writes here
```

This job writes with `mode("overwrite")`, which deletes everything under its
prefix first. If the deltas lived in the same folder, re-running the full load
would delete every incremental file ever written, and nothing would say so.

Separate folders mean an overwrite can only destroy its own output.

---

## incremental_load.py

Reads only the rows that changed, writes them to S3, moves the watermark.

```
1. read the watermark from DynamoDB      "last time I got to 13:53"
2. read rows changed since then          WHERE updated_at > 13:53 - 15 min
3. write them to .../delta/ as Parquet
4. move the watermark
```

The five decisions in it are written out in the file's own docstring. The three
that matter most:

### The watermark is the time the read started

Same reason as the full load. A row that changes during the read might be read
before or after its change, so the next run has to look at that period again.

### The window starts from the watermark, not from now

Two filters that look almost identical:

```
updated_at > watermark - 15 minutes      correct
updated_at > now() - 15 minutes          wrong
```

If the pipeline was down for two hours, the first one reads all two hours. The
second one reads the last 15 minutes and skips the rest without mentioning it.

### The watermark moves after the write succeeds

If the watermark moved first and the write then failed, the next run would
start after rows that never landed. Moving it last means a failed run is simply
repeated.

### What it deliberately does not do

There is no de-duplication in this file.

Within one run, `measurement_id` is the primary key, so every row comes back
exactly once. A dedup step here would run on every row and remove nothing.

The duplicates come from the 15 minute overlap *between* runs. Resolving them
needs more than one run's output in the same place, which is Snowflake. So it
happens in `sql/snowflake/05_silver.sql`.

---

## Three safeguards

These handle conditions that do not happen at 180 rows a run. That is exactly
why they are easy to leave out, and expensive to add after something has gone
wrong.

### 1. The read is batched

180 rows fit in memory. A six hour backlog after an outage might not.

`--batch-rows` (default 200,000) sets how many rows come back at a time, and
each batch is written as its own file. At 180 rows there is one batch, so the
normal case is unchanged.

The subtlety is which kind of cursor. An ordinary Postgres cursor sends the
whole result to the client when the query runs, so fetching in batches would
save nothing: the rows are already here. This uses a **named** cursor, which
leaves the rows on the server and hands over one batch at a time. A named
cursor has to run inside a transaction, which is why the session does not use
autocommit.

**If a batch fails partway through.** Say batch 4 of 9 fails. Batches 1 to 3
are already in S3 and the watermark has not moved. The next run reads the whole
window again and writes those rows a second time. Duplicates are already
handled in silver. The alternative, half a window with a moved watermark, would
be a hole nobody could find.

**What it costs.** The read transaction stays open while the files are written,
so a long backlog holds a transaction open on the source database and vacuum
cannot clean up behind it. At a normal 180 rows that is one second. If backlogs
became routine, the batches would be written to local disk first and uploaded
after the connection closed.

### 2. The watermark write is conditional

```python
ConditionExpression="watermark = :seen"
```

This says: only write if the watermark is still the value I read. If another
run moved it in the meantime, this write fails and the job stops with a message
saying so, instead of overwriting it.

This is the reason the watermark lives in DynamoDB and not in a file in S3. S3
cannot do a conditional write on a value. Two overlapping runs writing to S3
would both succeed, and the later one would win, silently.

The file name carries the run id and a part number for the same reason:

```
incr_20261006T221500Z_a1b2c3d4_p000.parquet
     ^window end       ^run id  ^part
```

`window_end` on its own is not unique. Two runs started in the same second
would write to the same key and the second would overwrite the first.
`MaxConcurrentRuns = 1` stops that happening today, but that is a setting on
the job, not a property of the code, and settings get changed.

### 3. The column list is compared each run

The list of columns is stored in DynamoDB next to the watermark. Each run
compares what Postgres returned against what was stored.

A column added in Postgres would otherwise never appear in S3 and nothing would
mention it. When the list changes, the job prints what was added and removed
and records it in the run log.

It does not fail the run. A harmless added column should not stop ingestion.

It needs one run to establish a baseline, so drift is detected from the second
run onwards.

---

## Running the incremental job locally

Useful for testing a change before uploading it. It reads the `PG*` environment
variables, the same ones `psql` uses.

```
source db.env
python3 pipeline/incremental_load.py --s3-bucket <BUCKET> --dry-run
```

`--dry-run` reads the window, reports how many rows it found and how many files
it would write, and writes nothing. It does not move the watermark.

It still needs AWS credentials, because it reads the watermark from DynamoDB
before it does anything else.
