-- 04_bronze.sql
-- Append only. Every version of every row is kept; the current one is
-- resolved in silver. Two COPY statements because the snapshot and the
-- deltas do not share a schema.

USE WAREHOUSE poc_wh;
USE DATABASE chrysos_poc;
USE SCHEMA bronze;

ALTER SESSION SET TIMEZONE = 'UTC';

CREATE TABLE IF NOT EXISTS bronze.assay_measurement (
  measurement_id     NUMBER(38,0),
  sample_id          VARCHAR,
  machine_id         NUMBER(38,0),
  customer           VARCHAR,
  element            VARCHAR,
  value_ppm          NUMBER(20,6),
  uncertainty_ppm    NUMBER(20,6),
  measure_seconds    NUMBER(38,0),
  assay_mode         VARCHAR,
  sample_type        VARCHAR,
  crm_code           VARCHAR,
  crm_certified_ppm  NUMBER(20,6),
  below_detection    BOOLEAN,
  started_utc        TIMESTAMP_NTZ,
  created_at         TIMESTAMP_NTZ,
  updated_at         TIMESTAMP_NTZ,
  dw_ingested_at     VARCHAR,          -- lineage, delta rows only
  dw_window_start    VARCHAR,
  dw_window_end      VARCHAR,
  dw_job_run_id      VARCHAR,
  dw_source          VARCHAR,
  dw_loaded_at       TIMESTAMP_NTZ DEFAULT SYSDATE()
);

COPY INTO bronze.assay_measurement (
  measurement_id, sample_id, machine_id, customer, element,
  value_ppm, uncertainty_ppm, measure_seconds, assay_mode, sample_type,
  crm_code, crm_certified_ppm, below_detection,
  started_utc, created_at, updated_at, dw_source
)
FROM (
  SELECT
    $1:measurement_id::NUMBER(38,0),
    $1:sample_id::VARCHAR,
    $1:machine_id::NUMBER(38,0),
    $1:customer::VARCHAR,
    $1:element::VARCHAR,
    $1:value_ppm::NUMBER(20,6),
    $1:uncertainty_ppm::NUMBER(20,6),
    $1:measure_seconds::NUMBER(38,0),
    $1:assay_mode::VARCHAR,
    $1:sample_type::VARCHAR,
    $1:crm_code::VARCHAR,
    $1:crm_certified_ppm::NUMBER(20,6),
    $1:below_detection::BOOLEAN,
    $1:started_utc::TIMESTAMP_NTZ,
    $1:created_at::TIMESTAMP_NTZ,
    $1:updated_at::TIMESTAMP_NTZ,
    'snapshot'
  FROM @bronze.stg_s3/assay_measurement/snapshot/
)
FILE_FORMAT = (FORMAT_NAME = bronze.ff_parquet)
ON_ERROR = ABORT_STATEMENT;

COPY INTO bronze.assay_measurement (
  measurement_id, sample_id, machine_id, customer, element,
  value_ppm, uncertainty_ppm, measure_seconds, assay_mode, sample_type,
  crm_code, crm_certified_ppm, below_detection,
  started_utc, created_at, updated_at,
  dw_ingested_at, dw_window_start, dw_window_end, dw_job_run_id, dw_source
)
FROM (
  SELECT
    $1:measurement_id::NUMBER(38,0),
    $1:sample_id::VARCHAR,
    $1:machine_id::NUMBER(38,0),
    $1:customer::VARCHAR,
    $1:element::VARCHAR,
    $1:value_ppm::NUMBER(20,6),
    $1:uncertainty_ppm::NUMBER(20,6),
    $1:measure_seconds::NUMBER(38,0),
    $1:assay_mode::VARCHAR,
    $1:sample_type::VARCHAR,
    $1:crm_code::VARCHAR,
    $1:crm_certified_ppm::NUMBER(20,6),
    $1:below_detection::BOOLEAN,
    $1:started_utc::TIMESTAMP_NTZ,
    $1:created_at::TIMESTAMP_NTZ,
    $1:updated_at::TIMESTAMP_NTZ,
    $1:_ingested_at::VARCHAR,
    $1:_window_start::VARCHAR,
    $1:_window_end::VARCHAR,
    $1:_job_run_id::VARCHAR,
    'delta'
  FROM @bronze.stg_s3/assay_measurement/delta/
)
FILE_FORMAT = (FORMAT_NAME = bronze.ff_parquet)
ON_ERROR = ABORT_STATEMENT;

-- COPY keeps 64 days of load history and skips files it has already seen, so
-- re-running this picks up only files that arrived since. Same job the
-- DynamoDB watermark does on the Glue side, at a different layer.

CREATE TABLE IF NOT EXISTS bronze.machine (
  machine_id          NUMBER(38,0),
  machine_code        VARCHAR,
  site_name           VARCHAR,
  country             VARCHAR,
  customer            VARCHAR,
  utc_offset_hours    NUMBER(4,1),
  target_utilisation  NUMBER(4,3),
  installed_date      DATE,
  created_at          TIMESTAMP_NTZ,
  updated_at          TIMESTAMP_NTZ,
  dw_loaded_at        TIMESTAMP_NTZ DEFAULT SYSDATE()
);

COPY INTO bronze.machine (
  machine_id, machine_code, site_name, country, customer,
  utc_offset_hours, target_utilisation, installed_date, created_at, updated_at
)
FROM (
  SELECT
    $1:machine_id::NUMBER(38,0),
    $1:machine_code::VARCHAR,
    $1:site_name::VARCHAR,
    $1:country::VARCHAR,
    $1:customer::VARCHAR,
    $1:utc_offset_hours::NUMBER(4,1),
    $1:target_utilisation::NUMBER(4,3),
    $1:installed_date::DATE,
    $1:created_at::TIMESTAMP_NTZ,
    $1:updated_at::TIMESTAMP_NTZ
  FROM @bronze.stg_s3/machine/snapshot/
)
FILE_FORMAT = (FORMAT_NAME = bronze.ff_parquet)
ON_ERROR = ABORT_STATEMENT;

SELECT dw_source,
       COUNT(*) AS row_count,
       COUNT(DISTINCT measurement_id) AS ids,
       MIN(updated_at) AS oldest,
       MAX(updated_at) AS newest
FROM bronze.assay_measurement
GROUP BY dw_source
ORDER BY dw_source;

SELECT COUNT(*) AS machines FROM bronze.machine;
