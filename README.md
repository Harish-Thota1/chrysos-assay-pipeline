# Chrysos PhotonAssay POC: incremental ingestion

Extends the September POC with the three changes suggested at interview:
generate the data in AWS Glue, use a watermark for incremental loading into
S3, and handle deletes.

The original POC landed one immutable file per day into S3 and read it from
Snowflake. Incremental loading and delete handling only mean something against
a source that mutates, so this adds a relational source system and rebuilds
the ingestion path around it. The model and the finding are unchanged.

```
Postgres (RDS)  →  Glue  →  S3 Parquet  →  Snowflake  →  dim_machine
                                                          fact_assay_run
                                                          machine_reading
   NEW              NEW         NEW            unchanged
```

## The three changes

| Asked for | Built | Evidence |
|---|---|---|
| Generate the data in AWS Glue | Python Shell job on a 15 minute trigger | 100+ runs, unattended |
| Watermark for incremental loading | DynamoDB control table, read-start watermark, 15 minute look-back | 100.000% of rows current, verified against the source |
| Handle deletes | **Not built.** Measured and designed. | 1,932 stale rows, 0.856%, growing 172/hour |

## What the numbers say

Verified 2 October, after 31 unattended runs over 10 hours, by comparing
every `(measurement_id, updated_at)` pair in S3 against the live source:

```
rows S3 is responsible for    223,746      updated_at <= watermark
CURRENT                       223,746
BEHIND                              0
MISSING                             0
                              100.000%
```

And separately against the generator's own change log, which is written by the
thing making the changes and knows nothing about the pipeline:

```
expected in coverage    635
MISSING                   0
duplicates from overlap 152      removed in silver, not in bronze
```

Cost per incremental run, moving 152 rows:

```
0.0625 DPU x 40 s x $0.44/DPU-hour  =  $0.0003
```

Against the full load, which took 125 seconds on 2 workers to move the same
table. The full load's cost grows with the size of the table. The incremental
load's cost grows with how much changed. Only one of those keeps growing.

## Deletes: the gap, with a number on it

A deleted row is not in any query result, so no `WHERE updated_at > watermark`
can find it. It stays in S3 and nothing in the pipeline will ever contradict
it.

Measured against the generator's change log:

```
1,932 stale rows      1 hour after the snapshot
0.856% of the table
172 rows per hour     and it never decreases
```

That is 4,100 a day. Four days after a snapshot it is around 7%.

The fix is key reconciliation: take every key in the source, take every key in
S3, and whatever is in the second and not the first has been deleted. It needs
a full scan on both sides, so it runs daily rather than every 15 minutes,
which means deletes are detected within a day rather than within a quarter of
an hour. `verification/verify_current.py` already does this comparison; moving
it into the pipeline and writing the result as delete markers is the work that
was not done.

## The question is unchanged, and it runs on the September data

Which machines are drifting out of calibration, and how early can we tell. It
is still answered by `core.fact_assay_run` from the original load, which
carries a year of history with the drift modelled into it. Nothing in the
rebuilt ingestion path touches that table.

**The new source cannot answer it, and that is worth being plain about.** It
carries 90 days rather than a year, and it was generated to exercise ingestion
faults, nulls and absurd values and zero durations, not calibration drift. Run
the drift query against it and the answer is flat: there is no drift in there
to find.

In production these would be one thing. The pipeline would feed the fact table
and the question would always run on current data. Joining them here is a
re-seed of the source with a year of history and the drift built in, which is
a data task rather than a pipeline change, and it is not done.

So two claims, neither borrowing from the other:

```
the September data   answers the business question
the new path         is proven to move data without losing any, measured
```

## What else is not built

```
Snowpipe        bronze loads on a manual run, not on an S3 event
alerting        a failed Glue run is silent. No CloudWatch alarm.
dbt             the checks are hand written Python, not declared tests
Terraform       the AWS resources were built by hand and documented in infra/
compaction      ~100 small parquet files a day, no nightly rewrite
```

Snowpipe is the biggest of those and about an hour of work.

## Layout

```
pipeline/           what moves data
  full_load.py          Spark. one snapshot, seeds the first watermark.
  incremental_load.py   Python Shell. watermark, look-back, append.

simulator/          NOT the pipeline. stands in for the real source system.
  change_source.py      inserts, updates and deletes every 15 minutes,
                        and writes a ground truth file of every id it touched.

verification/       how the pipeline is graded
  reconcile.py          output vs the generator's change log
  verify_current.py     output vs the live source, pair by pair

sql/postgres/       the source schema and seed
sql/snowflake/      01 to 07, run in order. 00_run_all.sql runs them all.
infra/setup.md      every AWS resource as a command, with placeholders
docs/               the verification output
```

## The three design decisions worth reading the code for

**The watermark is the read start time, never the finish time.** A row changed
during the read may be read before or after its change. Taking the start time
means it is read again next run. Taking the finish time means it falls in a
gap and nothing ever looks for it. Re-reading is work you can see; a gap is
silent and permanent.

**The look-back window is measured from the watermark, not from now.**
`updated_at > watermark - 15 minutes` recovers a two hour backlog on its own.
`updated_at > now() - 15 minutes` reads the last fifteen minutes and skips the
rest without mentioning it. The two lines look almost identical.

**The watermark moves after the write succeeds, never before.** It records
what was successfully written, not what was successfully read.

## Running it

Nothing here runs against someone else's account. `infra/setup.md` lists every
AWS resource in the order it has to be created, with `<BUCKET>` and
`<ACCOUNT_ID>` as placeholders, so the build is reproducible rather than
described.

```
sql/postgres/01_schema.sql      the source
sql/postgres/02_seed.sql        90 days of history
pipeline/full_load.py           one snapshot, seeds the watermark
pipeline/incremental_load.py    every 15 minutes thereafter
sql/snowflake/00_run_all.sql    S3 into Snowflake, bronze and silver
verification/verify_current.py  prove nothing was lost
```

`db.env` and `snow.env` hold credentials and are gitignored. Nothing in this
repository contains a password, an account id or an ARN.
