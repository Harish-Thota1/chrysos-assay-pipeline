-- 07_q1_drift.sql
-- Which machines are drifting out of calibration, and how early can we tell?
--
-- Read only. Runs against core.fact_assay_run and raw.machine_reading.
--
-- The two tables are at different grains, 37M measurements against 125M
-- sensor readings every 15 seconds, so each is collapsed to one row per
-- machine per month before they meet. Both sides come down to ~720 rows
-- first, which is also the fast way round.

USE WAREHOUSE poc_wh;
USE DATABASE chrysos_poc;
USE SCHEMA core;

WITH assay AS (
  -- crm only: a customer's rock has no certificate, so a wrong reading on one
  -- can never be detected. HAVING drops the stub months at each end, where a
  -- fleet spread over five continents leaves a handful of rows in UTC.
  SELECT
    f.machine_key,
    DATE_TRUNC('month', f.started_at_utc)::DATE AS month,
    COUNT(*) AS crm_runs,
    AVG(100.0 * (f.value_ppm - f.crm_certified_ppm)
        / f.crm_certified_ppm) AS pct_off_certificate
  FROM core.fact_assay_run f
  WHERE f.sample_type = 'crm'
    AND f.element = 'Gold'
  GROUP BY 1, 2
  HAVING COUNT(*) > 100
),
telemetry AS (
  -- status = 'RUNNING' is load bearing. An idle machine sits cooler, so
  -- mixing idle and working readings makes a busy month look hot for reasons
  -- that have nothing to do with the machine's health.
  SELECT
    r.machine_id,
    DATE_TRUNC('month', r.reading_time_utc)::DATE AS month,
    AVG(r.detector_temp_c) AS avg_detector_temp,
    AVG(r.beam_current_ua) AS avg_beam_current
  FROM raw.machine_reading r
  WHERE r.status = 'RUNNING'
  GROUP BY 1, 2
)
-- Three tables, because the two facts name the machine differently:
-- fact_assay_run has machine_key = 17, machine_reading has 'PA-017'.
-- dim_machine holds both. Join key is machine AND month; either alone would
-- match March's temperature to June's readings.
SELECT
  a.month,
  m.machine_id,
  a.crm_runs,
  ROUND(a.pct_off_certificate, 2) AS pct_off_certificate,
  ROUND(t.avg_detector_temp, 2) AS detector_temp_c,
  ROUND(t.avg_beam_current, 1) AS beam_current_ua
FROM assay a
JOIN core.dim_machine m ON m.machine_key = a.machine_key
JOIN telemetry t ON t.machine_id = m.machine_id
                AND t.month = a.month
WHERE m.machine_id IN ('PA-017', 'PA-018')
ORDER BY a.month, m.machine_id;

-- PA-017 climbs 0.5 -> 10.4 pct off, 24.9 -> 29.3 degrees, beam 114.7 -> 106.4.
-- PA-018 moves on none of the three.
--
-- In the first month PA-017 is COOLER than PA-018. On day one they cannot be
-- told apart; the noise is bigger than the fault. That is the point.
--
-- Per reading, noise is 1.5% and month-one drift is 0.5%, so no alarm on a
-- single result can fire. Across 504 reference samples the error on the mean
-- is 1.5% / sqrt(504) = 0.067%, so 0.48% is seven standard errors out.
-- The machine cannot detect its own drift. The warehouse can, because it can
-- group a month together.


-- The whole fleet, ranked. This is the version for a dashboard.
WITH assay AS (
  SELECT
    f.machine_key,
    DATE_TRUNC('month', f.started_at_utc)::DATE AS month,
    COUNT(*) AS crm_runs,
    AVG(100.0 * (f.value_ppm - f.crm_certified_ppm)
        / f.crm_certified_ppm) AS pct_off
  FROM core.fact_assay_run f
  WHERE f.sample_type = 'crm'
    AND f.element = 'Gold'
  GROUP BY 1, 2
  HAVING COUNT(*) > 100
)
SELECT
  m.machine_id,
  m.site_name,
  m.country,
  COUNT(*) AS months,
  ROUND(MIN(a.pct_off), 2) AS first_month_pct,
  ROUND(MAX(a.pct_off), 2) AS worst_month_pct,
  ROUND(MAX(a.pct_off) - MIN(a.pct_off), 2) AS drift_over_year
FROM assay a
JOIN core.dim_machine m ON m.machine_key = a.machine_key
GROUP BY 1, 2, 3
ORDER BY drift_over_year DESC
LIMIT 10;

-- PA-034, PA-052 and PA-017 around 10 to 11. Everything else under 0.5.
-- Three machines out of sixty, found by the data rather than by anyone
-- going to look.
