-- 01_setup.sql
-- Schemas, file format and session timezone.
--
--   raw     sensor readings, one row every 15 seconds, as the machines sent them
--   bronze  measurements as they land from S3. append only
--   silver  one row per measurement_id, the copy with the latest updated_at
--   gold    dim_machine, dim_customer, fact_assay_run
--   core    the published model and the drift analysis

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE poc_wh;
USE DATABASE chrysos_poc;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Snapshot and delta files are both Parquet, so one format serves both.
CREATE FILE FORMAT IF NOT EXISTS bronze.ff_parquet TYPE = PARQUET;

-- Spark writes timestamps with no timezone, pyarrow tags them UTC. Setting this
-- lands both on the same instant. 04 sets it again so it can be run on its own.
ALTER SESSION SET TIMEZONE = 'UTC';
