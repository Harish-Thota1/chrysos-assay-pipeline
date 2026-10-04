-- 06_gold.sql
-- The dimensional model. Grain: one row per measurement_id. Built from silver.
-- core.fact_assay_run is a separate table and nothing here writes to it.
--
-- Full rebuild on every run. At this size it costs seconds, and a rebuild
-- cannot drift from its source the way an incremental merge can.

USE WAREHOUSE poc_wh;
USE DATABASE chrysos_poc;
USE SCHEMA gold;

-- The source calls a unit PA-017, everything else calls it 17. Nothing joins
-- without this in the middle.
CREATE OR REPLACE TABLE gold.dim_machine AS
SELECT
  machine_id       AS machine_key,
  machine_code     AS machine_id,
  site_name,
  country,
  customer         AS customer_name,
  utc_offset_hours AS tz_offset_hours,
  target_utilisation,
  installed_date
FROM bronze.machine;

-- Key 0 is the Chrysos test jar. Reference material has no customer, and a null
-- key would force an outer join on every query against the fact.
CREATE OR REPLACE TABLE gold.dim_customer AS
WITH named AS (
  SELECT customer AS customer_name
  FROM bronze.machine
  WHERE customer IS NOT NULL
  GROUP BY customer
),
keyed AS (
  -- Numbered in its own step. ROW_NUMBER starts at 1, so it cannot hit 0.
  SELECT ROW_NUMBER() OVER (ORDER BY customer_name) AS customer_key,
         customer_name
  FROM named
)
SELECT 0 AS customer_key, 'Chrysos test jar' AS customer_name
UNION ALL
SELECT customer_key, customer_name FROM keyed;

-- A null customer never matches the join, so COALESCE sends it to key 0.
-- That is the intent: crm rows are Chrysos jars, not customer work.
CREATE OR REPLACE TABLE gold.fact_assay_run AS
SELECT
  m.measurement_id,
  m.sample_id,
  m.machine_key,
  COALESCE(c.customer_key, 0) AS customer_key,
  TO_NUMBER(TO_CHAR(m.started_at_utc, 'YYYYMMDD')) AS date_key,
  m.started_at_utc,
  m.element,
  m.value_ppm,
  m.uncertainty_ppm,     -- stored, not derived: depends on grade and count time
  m.measure_seconds,
  m.service_mode,
  m.sample_type,
  m.crm_code,
  m.crm_certified_ppm,   -- copied on, so comparisons survive a cert revision
  m.below_lod
FROM silver.assay_measurement m
LEFT JOIN gold.dim_customer c ON c.customer_name = m.customer;


-- checks

-- grain_violations must be 0.
SELECT COUNT(*) AS row_count,
       COUNT(DISTINCT measurement_id) AS ids,
       COUNT(*) - COUNT(DISTINCT measurement_id) AS grain_violations
FROM gold.fact_assay_run;

SELECT COUNT(*) AS orphan_machine_keys
FROM gold.fact_assay_run f
LEFT JOIN gold.dim_machine d ON d.machine_key = f.machine_key
WHERE d.machine_key IS NULL;

-- crm_rows is here so a predicate that matches nothing cannot read as a pass.
SELECT COUNT_IF(LOWER(sample_type) = 'crm') AS crm_rows,
       COUNT_IF(LOWER(sample_type) =  'crm' AND crm_certified_ppm IS NULL)     AS crm_missing_cert,
       COUNT_IF(LOWER(sample_type) <> 'crm' AND crm_certified_ppm IS NOT NULL) AS non_crm_has_cert
FROM gold.fact_assay_run;

-- What the drift view has to work with. Run this before reading the view: an
-- empty view means no drift only if these counts are non-zero.
SELECT COUNT_IF(LOWER(sample_type) = 'crm')                       AS crm_rows,
       COUNT_IF(LOWER(element) = 'gold')                          AS gold_rows,
       COUNT_IF(LOWER(sample_type) = 'crm' AND LOWER(element) = 'gold'
                AND crm_certified_ppm > 0)                        AS rows_in_view,
       COUNT(DISTINCT DATE_TRUNC('month', started_at_utc))        AS months
FROM gold.fact_assay_run;


-- Calibration drift per machine per month, certified jars only.
-- One metal at a time: a gold error and a silver error are different scales.
-- Flat against this source. It carries 90 days and no calibration drift was
-- modelled into it, so there is nothing here to find. See the README.
CREATE OR REPLACE VIEW gold.v_calibration_drift AS
WITH per_month AS (
  SELECT machine_key,
         DATE_TRUNC('month', started_at_utc) AS month,
         AVG((value_ppm - crm_certified_ppm)
             / NULLIF(crm_certified_ppm, 0)) * 100 AS bias_pct,
         COUNT(*) AS certified_jars
  FROM gold.fact_assay_run
  WHERE LOWER(sample_type) = 'crm'
    AND LOWER(element) = 'gold'
    AND crm_certified_ppm > 0
  GROUP BY 1, 2
)
SELECT d.machine_id AS unit,
       d.site_name AS site,
       d.country,
       ROUND(MIN(p.bias_pct), 2) AS best_month_pct,
       ROUND(MAX(p.bias_pct), 2) AS worst_month_pct,
       ROUND(MAX(p.bias_pct) - MIN(p.bias_pct), 2) AS moved_pct,
       SUM(p.certified_jars) AS certified_jars,
       -- 5 points is the POC threshold, not a Chrysos specification.
       CASE WHEN MAX(p.bias_pct) - MIN(p.bias_pct) > 5
            THEN 'Service required' ELSE 'Within noise' END AS verdict
FROM per_month p
JOIN gold.dim_machine d ON d.machine_key = p.machine_key
GROUP BY 1, 2, 3
ORDER BY moved_pct DESC;
