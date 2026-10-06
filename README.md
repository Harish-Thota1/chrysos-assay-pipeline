# Chrysos PhotonAssay POC: incremental ingestion

This moves assay data from a database into a warehouse, every 15 minutes,
copying only the rows that changed.

```
Postgres (RDS)  →  AWS Glue  →  S3 Parquet  →  Snowflake
  the source        the job      the files      the warehouse
```

Four things are in this repo:

1. A pipeline that does the copying.
2. A generator that keeps changing the source, so there is something to copy.
3. Two scripts that check nothing was lost.
4. The setup steps for every AWS and Snowflake resource it needs.

---

## 1. The problem it solves

The simple way to copy a table is to copy all of it, every time. That works
until the table is large, and then you are moving 200,000 rows to pick up the
40 that changed.

An incremental load copies only what changed. To do that it has to remember how
far it got last time.

### What a watermark is

A watermark is a saved timestamp. Nothing more. It says "I have read everything
up to this moment."

It lives in a DynamoDB table, one row per source table:

```
table_name           watermark
assay_measurement    2026-10-02T13:53:03Z
```

Each run does four things:

```
1. read the watermark                    "last time I got to 13:53"
2. read rows changed since then          WHERE updated_at > 13:53
3. write those rows to S3
4. save a new watermark
```

That is the whole idea. The rest of this file is about the three places it can
go wrong.

---

## 2. The three things that can go wrong

### Problem 1: when do you take the timestamp?

A job that starts at 10:00 and finishes at 10:02 could save either time.

Say a row changes at 10:01, while the job is running. Whether the job happens
to read that row before or after the change is pure luck.

```
save the FINISH time (10:02)   next run reads rows after 10:02.
                               the 10:01 row is never read again. gone.

save the START time (10:00)    next run reads rows after 10:00.
                               the 10:01 row is read again. fine.
```

Reading a row twice is harmless, because duplicates get removed later. Missing
a row is permanent and silent.

**So the watermark is the time the read started, never the time it finished.**

### Problem 2: what if a run is late?

Runs are scheduled every 15 minutes, but a run can be skipped, or AWS can be
slow, or the job can fail. Then the next run has more than 15 minutes of
changes waiting for it.

There are two ways to write the filter, and they look almost the same:

```
updated_at > watermark - 15 minutes      correct
updated_at > now() - 15 minutes          wrong
```

The second one reads the last 15 minutes and ignores everything older. If the
pipeline was down for two hours, those two hours are skipped and nothing says
so.

The first one starts from where the pipeline actually got to, so a late run
catches up on its own.

**So the window is measured from the watermark, not from the current time.**

The `- 15 minutes` is a safety margin. Postgres stamps `updated_at` when a
transaction starts, but the row is not visible to anyone else until the
transaction commits, which is later. So a row can be stamped 13:52 and become
readable at 13:54, after a watermark of 13:53 has already moved past it.
Re-reading the last 15 minutes covers that.

### Problem 3: what if the write fails?

If the watermark moves and then the write to S3 fails, those rows are gone
forever. The next run starts after them.

**So the watermark moves only after the write succeeds.** It records what was
written, not what was read.

### Three more safeguards

Those three problems happen at any size. These three only happen at sizes this
POC does not reach, which is why they are easy to leave out.

```
the read is batched          180 rows fit in memory. a six hour backlog
                             after an outage might not. --batch-rows caps
                             it and each batch becomes its own file.

the watermark write is       only write if the watermark is still the value
conditional                  I read. if another run moved it, fail instead
                             of overwriting. this is why the watermark is
                             in DynamoDB and not a file in S3: S3 cannot do
                             a conditional write on a value.

the column list is           stored next to the watermark and compared each
compared                     run. a column added in Postgres would
                             otherwise never appear in S3 and nothing would
                             mention it.
```

`pipeline/README.md` has the detail on all three, including what each one
costs.

---

## 3. What was measured

Running unattended, two Glue jobs on 15 minute schedules, one generating
changes and one loading them.

### Check 1: is anything missing or out of date?

`verification/verify_current.py` takes every `(measurement_id, updated_at)` pair
in S3 and every pair in the live database, and compares them.

It compares pairs, not row counts, because two counts can match while the
contents differ. And it compares pairs, not just ids, because an id tells you
the row arrived, not that S3 has the current version of it.

Measured 2 October, after 31 runs over 10 hours:

```
rows S3 is responsible for    223,746      updated_at <= watermark
CURRENT                       223,746
BEHIND                              0      <- must be 0
MISSING                             0      <- must be 0
                              100.000%
```

`BEHIND` means S3 has an older version of a row than the database does.
`MISSING` means S3 does not have the row at all. Both are zero.

### Check 2: does an independent record agree?

The generator writes a log of every row it touches, to
`s3://<bucket>/change_log/`. That log is written by the thing making the
changes. It knows nothing about the pipeline.

`verification/reconcile.py` compares the pipeline's output against that log:

```
expected in coverage    635
MISSING                   0      <- must be 0
duplicates from overlap 152      removed later, in silver
```

This matters because the pipeline is not checking itself. If the pipeline and
the generator's log agree, that is two separate records of the same events
lining up.

### What it costs

```
one incremental run     0.0625 DPU x 40 s x $0.44/DPU-hour  =  $0.0003
the full load           2 workers x 125 s                   =  $0.06
```

A DPU is a unit of Glue compute. 0.0625 is the smallest you can ask for, and it
is enough, because the job moves about 150 rows.

The point of the comparison: the full load's cost grows as the table grows. The
incremental load's cost grows with how much changed. Only the first one keeps
getting more expensive.

---

## 4. Deletes are not handled

This is the one thing in the list that is not finished.

When a row is deleted from the source, it is gone. No query can find it:

```sql
SELECT * FROM assay_measurement WHERE updated_at > '13:53'
```

A deleted row has no `updated_at` to compare, because it has no row. So the
copy in S3 stays there, and nothing in the pipeline will ever contradict it.

Measured:

```
1,932 stale rows      one hour after the snapshot
0.856% of the table
172 rows per hour     and it never goes down
```

That is 4,100 a day. After four days around 7% of S3 is rows that no longer
exist in the source.

### How it would be fixed

Compare the full list of keys on both sides.

```
every key in the source      {1, 2, 3, 5}
every key in S3              {1, 2, 3, 4, 5}
in S3 but not the source     {4}            <- deleted
```

That needs a full scan of both sides, which is expensive, so it would run once
a day rather than every 15 minutes. Deletes would be found within a day instead
of within 15 minutes.

`verification/verify_current.py` already does exactly this comparison, which is
where the 1,932 came from. Moving it into the pipeline and writing the result
back as delete markers is the work that was not done.

---

## 5. The business question

**Which machines are drifting out of calibration, and how early can we tell?**

A PhotonAssay machine measures gold in a sample. To check it is still accurate,
the lab runs a certified reference jar through it, where the true answer is
known. If the machine reads 10% high on a jar whose value is certified, the
machine is wrong, not the jar.

`sql/snowflake/07_q1_drift.sql` groups those reference runs by machine and by
month, and asks how far off certificate each machine was.

The answer: PA-034, PA-052 and PA-017 are 10 to 11% off. The other 57 machines
are under 0.5%.

The interesting part is the first month. PA-017 is only 0.5% off, and a single
measurement has about 1.5% noise in it. So no individual result looks wrong. No
alarm on the machine itself could fire. But averaged over 504 reference samples
in that month, the noise shrinks to 0.067%, and 0.5% is then seven times larger
than the uncertainty.

The machine cannot detect its own drift. The warehouse can, because it can
average a month at a time.

### Which data answers it

Two sets of data live in this warehouse, and they do different jobs.

```
core, raw        a year of history, with drift in three machines.
                 answers the question above.

bronze → gold    the live source, loaded by this pipeline.
                 proves the pipeline does not lose rows.
```

**The live source cannot answer the question.** It holds 90 days, not a year,
and it was built to produce bad data on purpose (nulls, impossible values,
zero-second measurements) so the pipeline has something hard to carry. No
calibration drift was put into it. Run the drift query against it and the
result is flat, because there is nothing there to find.

Keeping them separate means neither result depends on the other. The drift
finding does not rely on the pipeline being correct, and the pipeline's
100.000% does not rely on the drift data existing.

**One thing to know before you run file 07.** It reads `core.fact_assay_run`
and `raw.machine_reading`, and this repo does not create them. Against an empty
warehouse it fails with "table does not exist". Files `01` to `06` need nothing
but the pipeline's own output and run on a clean account.

---

## 6. What is in each folder

```
pipeline/       the two jobs that move data
simulator/      keeps changing the source so there is something to move
verification/   checks that nothing was lost
sql/            the source database, and the warehouse
infra/          how to build the AWS side
docs/           how to run it, and the measured results
```

Each folder has its own README.

### pipeline/

```
full_load.py          runs once. copies the whole table, saves the first
                      watermark.
incremental_load.py   runs every 15 minutes. copies what changed.
```

### simulator/

```
change_source.py      every 15 minutes: inserts ~120 rows, updates 20,
                      deletes 40, and logs every id it touched.
```

**This is not part of the pipeline.** It stands in for the lab's own system.
Chrysos would not own that system and could not change it, so nothing in
`pipeline/` is allowed to assume anything about it beyond an `updated_at`
column. Keeping it in a separate folder is how that stays true.

### verification/

```
verify_current.py     S3 against the live database, pair by pair
reconcile.py          S3 against the generator's log
```

Both are read only and both run on a laptop. Section 3 above is their output.

### sql/

```
postgres/    01_schema.sql, 02_seed.sql        the source. run once.
snowflake/   00_run_all.sql                    runs 01 to 07 in order
             01_setup.sql                      schemas, file format, timezone
             02_integration.sql                lets Snowflake read the bucket
             03_stage.sql                      points Snowflake at the files
             04_bronze.sql                     loads the files, as they are
             05_silver.sql                     one row per measurement
             06_gold.sql                       the dimensional model
             07_q1_drift.sql                   the business question
```

The three warehouse layers, in plain terms:

```
bronze    everything that arrived, including duplicates. nothing deleted.
silver    one row per measurement, the newest version of it.
gold      tables shaped for questions: facts and dimensions.
```

Bronze keeps duplicates on purpose. The 15 minute look-back from section 2
means most rows arrive twice, and bronze is the record of what landed. Silver
is where the newest copy of each row is picked out. If a bug is found in that
choice, bronze still has everything and silver can be rebuilt.

---

## 7. How to run it

**`docs/how-to-run.md` is the full runbook.** Seven phases, each one saying what
you should see before moving on, and a table of what the common errors mean.

```
1  build the AWS resources     ~45 min    infra/setup.md
2  create the source database  ~10 min    sql/postgres/
3  upload the job code to S3   ~5 min     Glue runs code from S3, not a laptop
4  run the full load once      ~3 min     saves the first watermark
5  turn on the two schedules   ~2 min     generator and loader, 15 min apart
6  load it into Snowflake      ~15 min    sql/snowflake/01 to 07
7  run the two checks          ~5 min     verification/
```

First time through is about two hours and most of that is phase 1. After phase
5 the pipeline runs on its own, and you only come back for phase 7.

`infra/setup.md` lists every AWS resource as a command, in the order it has to
be created, with `<BUCKET>` and `<ACCOUNT_ID>` as placeholders.

Credentials go in `db.env` and `snow.env`, which are gitignored. Nothing in
this repository contains a password, an account id, a bucket name or an ARN.

---

## 8. What is not built

```
delete handling   section 4. measured at 0.856%, growing 172 rows an hour.
Snowpipe          bronze loads when you run it, not when a file arrives.
alerting          a failed Glue run is silent. no CloudWatch alarm.
dbt               the checks are Python scripts, not declared tests.
Terraform         the AWS resources were built by hand, documented in infra/.
compaction        ~100 small Parquet files a day, no nightly rewrite.
```

Delete handling is the real gap. Snowpipe is the next largest and is about an
hour of work.
