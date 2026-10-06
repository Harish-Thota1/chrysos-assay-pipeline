# sql/

Two databases. They never talk to each other directly. Everything between them
goes through S3.

```
postgres/     the source system. the pipeline reads FROM here.
snowflake/    the warehouse. it reads FROM S3.
```

---

## postgres/

Run once, at setup.

```
01_schema.sql    tables, CHECK constraints, an index on updated_at
02_seed.sql      90 days of history: 60 machines, ~217,000 measurements
```

```
source db.env
psql -f sql/postgres/01_schema.sql
psql -f sql/postgres/02_seed.sql
```

`psql` picks up the `PG*` variables from `db.env`, so there is no connection
string to type.

`02_seed.sql` ends with a count. Check it before running the full load. A short
seed makes a short snapshot, and nothing downstream will tell you.

**These are not safe to re-run.** `01_schema.sql` fails on existing tables, on
purpose. Dropping the source would destroy the history the pipeline is being
measured against.

### Why the constraints are incomplete

The `CHECK` constraints catch obvious nonsense and let six specific fault types
through. See `simulator/README.md` for the list.

That is deliberate. Constraints only ever catch what someone thought of at
design time. Everything else arrives in the warehouse, and a warehouse that has
never had to deal with it is not a tested warehouse.

---

## snowflake/

Run in order, 01 to 07.

```
00_run_all.sql       runs 01 to 07 in one command, once the files are staged
01_setup.sql         schemas, Parquet file format, session timezone
02_integration.sql   lets Snowflake read the S3 bucket
03_stage.sql         points Snowflake at the folders, and lists them
04_bronze.sql        loads the files as they are
05_silver.sql        one row per measurement
06_gold.sql          dim_machine, dim_customer, fact_assay_run
07_q1_drift.sql      the business question
```

### Run them one at a time the first time

Not as a batch. The `LIST` output in `03` is the only thing that tells you
whether Snowflake can actually read the bucket, and `EXECUTE IMMEDIATE FROM`
hides it.

This matters because a `COPY` over an empty folder **succeeds** with zero rows
and no error. If you skip the `LIST`, the first sign of trouble is an empty
table three files later, and it looks like a bug in `04` rather than a
permissions problem in `02`.

Once it has worked once, later runs are one command.

### The three layers, in plain terms

```
bronze    everything that arrived, duplicates included. nothing deleted.
silver    one row per measurement, the newest version of it.
gold      tables shaped for questions: facts and dimensions.
```

**Why bronze keeps duplicates.** The pipeline re-reads the last 15 minutes
every run, so most rows arrive twice. Bronze is the record of what landed, not
a picture of what is true now.

**Why silver is a view, not a table.** Picking the current version of a row is
a decision, and decisions can turn out wrong. Keeping bronze complete means
that if the choice needs changing, silver can be rebuilt from what is already
there. Writing the choice into bronze would throw away the evidence.

### Why 04 has two COPY statements

Because the snapshot and the deltas are written by different tools and do not
have the same schema.

```
snapshot   written by Spark      timestamps with no timezone, decimal(12,4)
delta      written by pyarrow    timestamps tagged UTC, decimal(20,6),
                                 plus 4 lineage columns the snapshot lacks
```

The two `INFER_SCHEMA` queries at the end of `03` print the actual types out of
the files, so this is something you can check rather than something to take on
trust.

### Why the dedup is in 05 and not in the Glue job

Within one incremental run, `measurement_id` is the primary key, so no row
repeats. There is nothing to deduplicate.

The duplicates come from the overlap *between* runs. Spotting them needs
several runs' output in the same table, and silver is the first place that is
true.

It deduplicates on `measurement_id` and **not** on `(sample_id, element)`. That
pair genuinely repeats in the source when a sample is re-run. Collapsing it
would silently delete one row of every real duplicate.
`silver.source_duplicate` counts them instead.

### Which files are safe to re-run

```
01   IF NOT EXISTS throughout. never touches an object that holds data.
03   OR REPLACE. a stage is a pointer, not storage, so replacing it is free.
05   OR REPLACE. it is a view.
06   OR REPLACE. a full rebuild from silver, which takes seconds.
04   safe, and worth understanding why.
```

`COPY` keeps 64 days of load history and skips files it has already loaded. So
re-running `04` picks up only the deltas that arrived since, and loads nothing
twice. There is nothing to clear first.

That is the same job the DynamoDB watermark does on the Glue side, at a
different layer.

### 07 needs data this repo does not create

`07_q1_drift.sql` reads `core.fact_assay_run` and `raw.machine_reading`, which
hold a year of assay history and 15-second detector telemetry. Nothing in this
repo creates them. Against an empty warehouse it fails with "table does not
exist".

`01` to `06` build entirely from the pipeline's own output in S3 and need
nothing else.
