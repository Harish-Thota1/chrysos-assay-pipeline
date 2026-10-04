-- 03_stage.sql
-- External stage over the S3 prefixes the ingestion jobs write to.
--
--   assay_measurement/snapshot/   full_load.py, overwritten each run
--   assay_measurement/delta/      incremental_load.py, one new file per run
--   machine/snapshot/             60 rows

USE WAREHOUSE poc_wh;
USE DATABASE chrysos_poc;
USE SCHEMA bronze;

-- OR REPLACE is safe: a stage is a pointer, not storage. The COPY load history
-- that stops files loading twice sits on the table.
CREATE OR REPLACE STAGE bronze.stg_s3
  STORAGE_INTEGRATION = chrysos_poc_integration
  URL = 's3://<BUCKET>/bronze/'
  FILE_FORMAT = bronze.ff_parquet;

-- Reachability check. A COPY over an empty stage succeeds with zero rows.
LIST @bronze.stg_s3/assay_measurement/snapshot/;
LIST @bronze.stg_s3/assay_measurement/delta/;
LIST @bronze.stg_s3/machine/snapshot/;

-- Snapshot and delta do not share a schema, which is why 04 has two COPYs.
--   snapshot  Spark     no timezone, decimal(12,4)
--   delta     pyarrow   tagged UTC, decimal(20,6), lineage columns
SELECT * FROM TABLE(INFER_SCHEMA(
  LOCATION => '@bronze.stg_s3/assay_measurement/snapshot/',
  FILE_FORMAT => 'bronze.ff_parquet')) ORDER BY column_name;

SELECT * FROM TABLE(INFER_SCHEMA(
  LOCATION => '@bronze.stg_s3/assay_measurement/delta/',
  FILE_FORMAT => 'bronze.ff_parquet')) ORDER BY column_name;
