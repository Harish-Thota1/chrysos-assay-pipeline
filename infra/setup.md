# AWS setup

Every resource, in the order it has to be created. Region `ap-southeast-2`.
Replace `<BUCKET>` and `<ACCOUNT_ID>`.

Built by hand through the console and CLI rather than Terraform, which is the
honest record of what happened. At production scale this file would be
Terraform or CloudFormation.

## 1. S3

```
aws s3 mb s3://<BUCKET> --region ap-southeast-2
```

Prefixes used:

```
glue-scripts/     the job code. Glue reads from here at run time, not from a laptop.
glue-libs/        the psycopg2 wheel
bronze/<table>/snapshot/   full load output
bronze/<table>/delta/      incremental output
change_log/       the generator's ground truth, one JSON per run
_runlog/          one JSON per incremental run
```

## 2. RDS Postgres

```
aws rds create-db-instance \
  --db-instance-identifier chrysos-source \
  --db-instance-class db.t3.micro \
  --engine postgres \
  --master-username postgres \
  --master-user-password <PASSWORD> \
  --allocated-storage 20 \
  --publicly-accessible
```

Add your own IP to the security group so `psql` works from a laptop:

```
aws ec2 authorize-security-group-ingress \
  --group-id <SG_ID> --protocol tcp --port 5432 --cidr <YOUR_IP>/32
```

The default security group's self-referencing rule is what lets a Glue job
inside the VPC reach the database. Without it, a VPC-attached job is refused
even though both sit in the same AWS account.

Then `sql/postgres/01_schema.sql` and `02_seed.sql`.

## 3. IAM role for Glue

```
chrysos-glue-role
  AWSGlueServiceRole                  managed
  chrysos-s3-access                   inline, s3:GetObject/PutObject/ListBucket on the bucket
  chrysos-dynamodb-state              inline, GetItem/PutItem/UpdateItem on the one table
```

Trust policy allows `glue.amazonaws.com` to assume it.

Both inline policies name one resource each. Not `s3:*`, not `table/*`.

## 4. VPC endpoints

A Glue job attached to a VPC has no route to the internet. Anything it needs
must have an endpoint, or the call hangs until the job times out.

```
S3        gateway endpoint    free
DynamoDB  gateway endpoint    free
```

Those two are the only gateway endpoints AWS offers. Everything else is an
interface endpoint at about $7.30 a month, which is why the Glue API and
Secrets Manager are avoided rather than used.

**Attach the route table.** An endpoint created without one reports
`available` and routes nothing.

```
aws ec2 modify-vpc-endpoint --vpc-endpoint-id <EP> --add-route-table-ids <RTB>
aws ec2 describe-vpc-endpoints --query 'VpcEndpoints[*].[ServiceName,RouteTableIds]'
```

## 5. DynamoDB

```
aws dynamodb create-table \
  --table-name chrysos-pipeline-state \
  --attribute-definitions AttributeName=table_name,AttributeType=S \
  --key-schema AttributeName=table_name,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

One item per source table, holding the watermark.

## 6. Glue connection

```
chrysos-source-connection    JDBC, the Postgres host, a subnet and a security group
```

The subnet on this connection is what places a job inside the VPC. Listing it
in a job's `Connections` is the only thing that does.

## 7. Glue jobs

```
chrysos-generate-changes   pythonshell, 0.0625 DPU, simulator/change_source.py
chrysos-full-load          glueetl, 2 x G.1X, pipeline/full_load.py
chrysos-incremental-load   pythonshell, 0.0625 DPU, pipeline/incremental_load.py
```

All three have `MaxConcurrentRuns = 1` and the connection attached.

Parameters are passed as job arguments. Check them after saving:

```
aws glue get-job --job-name <NAME> --query 'Job.DefaultArguments' --output json
```

Every key must start with `--` and no key or value may have a leading or
trailing space. The console will accept both without complaining and the job
will fail with `GlueArgumentError` at line 22.

`--extra-py-files` points at the psycopg2 wheel in S3. Do not use
`--additional-python-modules`: it fetches from PyPI, and a VPC-attached job
cannot reach PyPI, so the job hangs for its full timeout.

```
pip download psycopg2-binary --platform manylinux2014_x86_64 \
  --python-version 3.9 --only-binary=:all: -d /tmp/wheels
aws s3 cp /tmp/wheels/psycopg2_binary-*.whl s3://<BUCKET>/glue-libs/
```

## 8. Triggers

```
chrysos-generate-every-15min      cron(0/15 * * * ? *)    :00 :15 :30 :45
chrysos-incremental-every-15min   cron(7/15 * * * ? *)    :07 :22 :37 :52
```

Offset by seven minutes so the load reads a settled database rather than
racing the thing writing to it.

Confirm the state is `ACTIVATED`, not `CREATED`. A trigger created without
`--start-on-creation` never fires.

**The triggers and the database go together.** Stopping RDS without stopping
the triggers gives you a failed run every 15 minutes all night.

## 9. Snowflake storage integration

```
STORAGE_AWS_ROLE_ARN = arn:aws:iam::<ACCOUNT_ID>:role/chrysos-poc-snowflake-role
STORAGE_ALLOWED_LOCATIONS = ('s3://<BUCKET>/')
```

The role needs `s3:GetObject`, `s3:GetObjectVersion`, `s3:ListBucket` and
`s3:GetBucketLocation`.

Its trust policy must contain the external id from `DESC INTEGRATION`. Each
integration gets its own, so a second integration against the same role fails
with `sts:AssumeRole` denied until its id is added too.

## Cost

```
generator          0.0625 DPU x 40 s x 96/day      ~$1.50/month
incremental load   0.0625 DPU x 40 s x 96/day      ~$1.50/month
full load          2 x G.1X x 125 s, once          ~$0.06
S3                 ~10 MB plus ~100 small files/day  cents
DynamoDB           a few hundred requests/day      free tier
RDS db.t3.micro                                    free tier for 12 months
```

The dominant cost is RDS once the free tier ends. Stop it when not in use, and
stop the triggers first.
