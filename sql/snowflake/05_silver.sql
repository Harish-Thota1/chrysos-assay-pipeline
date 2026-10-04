-- 05_silver.sql
-- One row per measurement, newest version. Column names match
-- core.fact_assay_run so queries written against the fact work here too.

USE WAREHOUSE poc_wh;
USE DATABASE chrysos_poc;
USE SCHEMA silver;

-- Dedup on measurement_id. Not (sample_id, element): that pair legitimately
-- repeats when a jar is re-keyed, and collapsing it would hide a real problem.
CREATE OR REPLACE VIEW silver.assay_measurement AS
SELECT
  measurement_id,
  sample_id,
  machine_id        AS machine_key,
  started_utc       AS started_at_utc,
  INITCAP(element)  AS element,          -- source writes 'gold', the fact uses 'Gold'
  value_ppm,
  uncertainty_ppm,
  measure_seconds,
  assay_mode        AS service_mode,
  sample_type,
  crm_code,
  crm_certified_ppm,
  below_detection   AS below_lod,
  customer,
  updated_at,
  dw_source,
  dw_job_run_id
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY measurement_id
           ORDER BY updated_at DESC,
                    CASE WHEN dw_source = 'delta' THEN 0 ELSE 1 END,
                    dw_loaded_at DESC
         ) AS rn
  FROM bronze.assay_measurement
)
WHERE rn = 1;

-- Deleted rows are still in here. Not fixed yet. Measured 2 Oct: 1,932 stale
-- rows, 0.856%, growing ~172/hour. Fix is one more predicate above:
--   AND measurement_id NOT IN (SELECT measurement_id FROM silver.deleted_key)

-- Same sample and element recorded twice in the source. Reported, not removed.
CREATE OR REPLACE VIEW silver.source_duplicate AS
SELECT sample_id,
       element,
       COUNT(*) AS row_count,
       COUNT(DISTINCT measurement_id) AS ids,
       MIN(measurement_id) AS first_id,
       MAX(measurement_id) AS later_id,
       MAX(updated_at) AS last_seen
FROM silver.assay_measurement
GROUP BY sample_id, element
HAVING COUNT(*) > 1;


-- checks

SELECT
  (SELECT COUNT(*) FROM bronze.assay_measurement) AS bronze_rows,
  (SELECT COUNT(*) FROM silver.assay_measurement) AS silver_rows,
  (SELECT COUNT(*) FROM bronze.assay_measurement)
    - (SELECT COUNT(*) FROM silver.assay_measurement) AS collapsed_by_overlap,
  (SELECT COUNT(*) FROM silver.source_duplicate) AS source_duplicates;

SELECT COUNT(*) AS row_count,
       COUNT(DISTINCT measurement_id) AS ids,
       COUNT(*) - COUNT(DISTINCT measurement_id) AS grain_violations
FROM silver.assay_measurement;

SELECT COUNT(*) AS orphan_machine_keys
FROM silver.assay_measurement s
LEFT JOIN core.dim_machine d ON d.machine_key = s.machine_key
WHERE d.machine_key IS NULL;

-- Certified value must be on every crm row and nowhere else.
SELECT COUNT_IF(sample_type =  'crm' AND crm_certified_ppm IS NULL)     AS crm_missing_cert,
       COUNT_IF(sample_type <> 'crm' AND crm_certified_ppm IS NOT NULL) AS non_crm_has_cert
FROM silver.assay_measurement;
