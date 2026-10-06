# How to run it

Seven phases, in order. Each one says what you should see before you move on.

First time through is about two hours, and most of that is phase 1. After that
the pipeline runs on its own and you only come back for phase 7.

```
1  AWS resources           ~45 min    one time
2  the source database     ~10 min    one time
3  job code into S3        ~5 min     repeat whenever you edit a job
4  the first snapshot      ~3 min     one time, seeds the watermark
5  turn on the schedules   ~2 min     one time
6  Snowflake               ~15 min    repeat to pick up new files
7  verify                  ~5 min     any time, as often as you like
```

---

## Before you start

| You need | Why |
|---|---|
| An AWS account, region `ap-southeast-2` | everything in phases 1 to 5 |
| A Snowflake account you can use `ACCOUNTADMIN` on | phase 6 creates a storage integration |
| `psql` | phase 2 |
| Python 3.9 or later | phase 7, on your own machine |
| `aws` CLI, configured | phases 1, 3, 4, 5 |

Phase 7 needs three Python packages locally. Nothing else in this repo runs on
your machine.

```
python3 -m venv .venv
source .venv/bin/activate
pip install boto3 psycopg2-binary pyarrow
```

Two files hold credentials. Create them yourself; both are gitignored and
neither is in this repo.

**`db.env`**

```
export PGHOST=your-rds-endpoint.ap-southeast-2.rds.amazonaws.com
export PGPORT=5432
export PGDATABASE=chrysos
export PGUSER=postgres
export PGPASSWORD=your-password
```

**`snow.env`**, only if you want to run Snowflake from the CLI rather than the
web UI. Phase 6 below uses the web UI, so this is optional.

---

## Phase 1: AWS resources

Follow `infra/setup.md` top to bottom. It lists every resource as a command,
in the order it has to be created, with `<BUCKET>` and `<ACCOUNT_ID>` as
placeholders.

What gets built:

```
S3 bucket                  holds the job code, the data and the change log
RDS Postgres               the source system
IAM role                   chrysos-glue-role, for the three Glue jobs
VPC endpoints              S3 and DynamoDB, both gateway, both free
DynamoDB table             chrysos-pipeline-state, holds the watermark
Glue connection            puts a job inside the VPC and holds the db password
3 Glue jobs                generator, full load, incremental load
2 Glue triggers            both every 15 minutes, offset by 7
```

**Stop and check before moving on.** The one that silently fails is the VPC
endpoints.

```
aws ec2 describe-vpc-endpoints \
  --query 'VpcEndpoints[*].[ServiceName,State,RouteTableIds]' --output table
```

Both rows must show a route table id. An endpoint created without one reports
`available` and routes nothing, and the symptom appears three phases later as
a job that hangs instead of failing.

---

## Phase 2: the source database

```
source db.env
psql -f sql/postgres/01_schema.sql
psql -f sql/postgres/02_seed.sql
```

`psql` picks up the `PG*` variables on its own, so there is no connection
string to type.

**What you should see.** The seed ends with a count. Roughly:

```
 machines | measurements | oldest_day | newest_day | crm_rows
----------+--------------+------------+------------+----------
       60 |      ~217000 | 90 days ago| today      |   ~18000
```

If `measurements` is small, stop. A short seed makes a short snapshot, and
nothing downstream will tell you.

---

## Phase 3: job code into S3

Glue runs the code from S3, not from your laptop. Re-run this every time you
edit a job.

```
aws s3 cp pipeline/full_load.py        s3://<BUCKET>/glue-scripts/
aws s3 cp pipeline/incremental_load.py s3://<BUCKET>/glue-scripts/
aws s3 cp simulator/change_source.py   s3://<BUCKET>/glue-scripts/
```

The two Python Shell jobs also need the `psycopg2` wheel, once:

```
pip download psycopg2-binary --platform manylinux2014_x86_64 \
  --python-version 3.9 --only-binary=:all: -d /tmp/wheels
aws s3 cp /tmp/wheels/psycopg2_binary-*.whl s3://<BUCKET>/glue-libs/
```

Do not use `--additional-python-modules` for this. It fetches from PyPI, and a
VPC-attached job has no route to PyPI, so the job hangs for its full timeout
instead of failing.

---

## Phase 4: the first snapshot

This runs once. It copies the whole table to S3 and writes the first
watermark, which every incremental run after it depends on.

```
aws glue start-job-run --job-name chrysos-full-load \
  --arguments '{"--bucket":"<BUCKET>","--prefix":"bronze","--tables":"assay_measurement,machine"}'
```

Takes about two minutes on 2 workers. Watch it:

```
aws glue get-job-runs --job-name chrysos-full-load \
  --query 'JobRuns[0].[JobRunState,ExecutionTime]' --output text
```

**What you should see.** `SUCCEEDED`, then files in both prefixes:

```
aws s3 ls s3://<BUCKET>/bronze/assay_measurement/snapshot/
aws s3 ls s3://<BUCKET>/bronze/machine/snapshot/
```

And a watermark in DynamoDB:

```
aws dynamodb get-item --table-name chrysos-pipeline-state \
  --key '{"table_name":{"S":"assay_measurement"}}'
```

If there is no watermark, do not go to phase 5. The incremental job has
nothing to start from.

---

## Phase 5: turn on the schedules

Two triggers, both every 15 minutes, offset by 7 so the load reads a settled
database rather than racing the thing writing to it.

```
aws glue start-trigger --name chrysos-generate-every-15min
aws glue start-trigger --name chrysos-incremental-every-15min
```

**What you should see.** Both `ACTIVATED`, not `CREATED`:

```
aws glue get-triggers --query 'Triggers[*].[Name,State,Schedule]' --output table
```

A trigger created without `--start-on-creation` sits in `CREATED` forever and
never fires.

From here the pipeline runs with nothing attached. Leave it an hour and come
back.

---

## Phase 6: Snowflake

The seven files in `sql/snowflake/` run in order. **The first time, run them
one at a time in a worksheet**, because the `LIST` output in `03` is the only
thing that tells you whether Snowflake can actually read S3, and running them
as a batch hides it.

```
01_setup.sql         schemas, file format, session timezone
02_integration.sql   the storage integration. needs a trust policy edit, see below
03_stage.sql         the external stage, plus LIST to prove S3 is reachable
04_bronze.sql        two COPY statements, append only
05_silver.sql        dedup to one row per measurement_id
06_gold.sql          dim_machine, dim_customer, fact_assay_run
07_q1_drift.sql      the business question
```

`02_integration.sql` has a step in the middle that is not SQL. After creating
the integration:

```sql
DESC INTEGRATION chrysos_poc_integration;
```

Copy `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` out of the
output, and put them in the trust policy of `chrysos-poc-snowflake-role` in
AWS. Each integration gets its own external id, so if you have two
integrations pointing at the same role, the role's trust policy needs both or
the second one fails with `sts:AssumeRole` denied.

**What you should see at each gate.**

`03` — three `LIST` results with files in them. Empty means Snowflake cannot
see the bucket. A `COPY` over an empty stage succeeds with zero rows and no
error, so this is the check that matters.

`04` — the counts at the bottom. `ids` lower than `row_count` on the delta
row is the look-back overlap and is expected.

`05` — `grain_violations` must be `0`.

`06` — `orphan_machine_keys` must be `0`, and `rows_in_view` on the last check
must be non-zero before you read the drift view.

`07` — **this one needs data the repo does not create.** It reads
`core.fact_assay_run` and `raw.machine_reading`, which hold a year of assay
history and 15-second telemetry. Against an empty warehouse it fails with
"table does not exist". `01` to `06` do not depend on it, so stop at `06` if
those tables are not there.

Once it has worked once, later runs are one command:

```sql
EXECUTE IMMEDIATE FROM @public.scripts/00_run_all.sql;
```

That needs the files uploaded to a stage first. `00_run_all.sql` says how at
the top.

`COPY` keeps 64 days of load history and skips files it has already seen, so
re-running `04` picks up only the deltas that arrived since. You do not have
to clear anything.

---

## Phase 7: verify

Two scripts, both read only, both on your own machine. They are the point of
the repo: they are what turns "it ran" into "nothing was lost".

**Is anything missing or out of date?**

```
source db.env
python3 verification/verify_current.py --bucket <BUCKET>
```

Compares every `(measurement_id, updated_at)` pair in S3 against the live
source. `BEHIND` and `MISSING` must both be `0`. Rows changed after the
watermark are reported as `NOT DUE YET`, not as failures.

**Does it agree with an independent record?**

```
python3 verification/reconcile.py --bucket <BUCKET>
```

Compares against the generator's own change log, written by the thing making
the changes, which knows nothing about the pipeline. `MISSING` must be `0`.

`docs/verification.md` has real output from both, with the numbers explained.

---

## Stopping it

**Order matters.** Stop the triggers first, then the database. The other way
round gives you a failed run every 15 minutes all night.

```
aws glue stop-trigger --name chrysos-generate-every-15min
aws glue stop-trigger --name chrysos-incremental-every-15min
aws rds stop-db-instance --db-instance-identifier chrysos-source
```

Running cost is about $3 a month of Glue plus RDS, which is inside the free
tier for the first 12 months. The dominant cost after that is RDS, so stop it
when you are not using it.

---

## When something goes wrong

The useful distinction is **a hang means networking, a refusal means
permissions.** A job that sits there until it times out is waiting for a route
that does not exist. A job that fails in seconds was told no.

| What you see | What it is |
|---|---|
| Job hangs, no error, times out | a VPC endpoint with no route table, or `--additional-python-modules` trying to reach PyPI |
| `GlueArgumentError` at line 22 | a job argument key without `--`, or a leading or trailing space. The console accepts both silently |
| Job connects nowhere, fails fast | the subnet is not on the job. Listing the Glue connection in `Connections` is the only thing that puts a job in the VPC |
| `password authentication failed` | the job's `--pgpassword` and `db.env` disagree. Read the value back with `aws glue get-job`, do not trust that Save worked |
| `No active warehouse selected` | you ran a Snowflake file on its own without `USE WAREHOUSE poc_wh` |
| `sts:AssumeRole` denied | the role's trust policy is missing that integration's external id |
| `COPY` succeeds, zero rows | the stage is pointing at a prefix with no files. Run the `LIST` in `03` |
| Trigger never fires | state is `CREATED`, not `ACTIVATED` |
| `verify_current.py` reports huge `BEHIND` | the full load has not run, or the watermark is missing from DynamoDB |

The deeper version of this is in `infra/setup.md`, next to the resource each
one belongs to.
