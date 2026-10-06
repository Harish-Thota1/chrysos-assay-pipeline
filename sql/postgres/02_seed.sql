-- ============================================================
--  Chrysos POC  ·  seed the source system
--
--  60 machines, 100,000 measurements, 200,000 measurement rows
--  (one row per measurement PER ELEMENT: gold and silver).
--
--  setseed() makes this reproducible. Run it twice, get the
--  same data twice.
-- ============================================================

BEGIN;

TRUNCATE assay_measurement RESTART IDENTITY;
DELETE FROM machine;

SELECT setseed(0.4242);

-- ------------------------------------------------------------
--  60 machines across 5 continents.
--  Customer split follows the published fleet: ALS 9, SGS 3,
--  Bureau Veritas 2, Intertek 1  ->  36 / 12 / 8 / 4 of 60.
-- ------------------------------------------------------------
INSERT INTO machine (machine_id, machine_code, site_name, country,
                     customer, utc_offset_hours, target_utilisation,
                     installed_date, created_at, updated_at)
SELECT
    g                                                       AS machine_id,
    'PA-' || lpad(g::text, 3, '0')                          AS machine_code,
    s.site_name,
    s.country,
    CASE WHEN g <= 36 THEN 'ALS'
         WHEN g <= 48 THEN 'SGS'
         WHEN g <= 56 THEN 'Bureau Veritas'
         ELSE              'Intertek' END                   AS customer,
    s.utc_offset_hours,
    -- APAC runs near 0.79, the rest of the fleet lower
    round((CASE WHEN s.country = 'Australia' THEN 0.74 ELSE 0.44 END
           + random() * 0.10)::numeric, 3)                   AS target_utilisation,
    date '2023-06-01' + (random() * 700)::int                AS installed_date,
    now() - interval '400 days',
    now() - interval '400 days'
FROM generate_series(1, 60) AS g
JOIN (VALUES
        (0, 'Perth Lab',        'Australia',      8.0),
        (1, 'Kalgoorlie Lab',   'Australia',      8.0),
        (2, 'Adelaide Lab',     'Australia',      9.5),
        (3, 'Val-d''Or Lab',    'Canada',        -5.0),
        (4, 'Sudbury Lab',      'Canada',        -5.0),
        (5, 'Antofagasta Lab',  'Chile',         -4.0),
        (6, 'Accra Lab',        'Ghana',          0.0),
        (7, 'Elko Lab',         'United States', -8.0)
     ) AS s(idx, site_name, country, utc_offset_hours)
  ON s.idx = g % 8;

-- ------------------------------------------------------------
--  200,000 measurement rows.
--
--  3 certified jars in every 40 samples, which is how a
--  minerals lab actually runs quality control.
-- ------------------------------------------------------------
INSERT INTO assay_measurement (
    sample_id, machine_id, customer, element, value_ppm, uncertainty_ppm,
    measure_seconds, assay_mode, sample_type, crm_code, crm_certified_ppm,
    below_detection, started_utc, created_at, updated_at)
SELECT
    CASE WHEN b.sample_type = 'crm'
         THEN 'CRM-' || lpad(b.n::text, 7, '0')
         ELSE 'SMP-' || lpad(b.n::text, 7, '0') END          AS sample_id,
    b.machine_id,
    mc.customer,
    e.element,
    v.value_ppm,
    round((0.05 * sqrt(v.value_ppm) + 0.005)::numeric, 4)    AS uncertainty_ppm,
    88 + (random() * 9)::int                                 AS measure_seconds,
    CASE WHEN random() < 0.94 THEN 'gold_tuned'
         ELSE 'silver_tuned' END                             AS assay_mode,
    b.sample_type,
    b.crm_code,
    CASE WHEN b.sample_type = 'crm' AND e.element = 'gold'
              THEN b.crm_cert
         WHEN b.sample_type = 'crm'
              THEN round((b.crm_cert * 18)::numeric, 4) END  AS crm_certified_ppm,
    v.value_ppm < CASE WHEN e.element = 'gold' THEN 0.02
                       ELSE 1.5 END                          AS below_detection,
    b.started_utc,
    b.started_utc + interval '2 minutes',
    b.started_utc + interval '2 minutes'
FROM (
    SELECT
        n,
        1 + (n % 60)                                         AS machine_id,
        now() - interval '90 days 10 minutes'
              + (n * interval '77.76 seconds')               AS started_utc,
        CASE WHEN n % 40 < 3 THEN 'crm' ELSE 'customer' END   AS sample_type,
        CASE n % 40 WHEN 0 THEN 'OREAS-LOW-01'
                    WHEN 1 THEN 'OREAS-MID-01'
                    WHEN 2 THEN 'OREAS-HIGH-01' END           AS crm_code,
        CASE n % 40 WHEN 0 THEN 0.5200
                    WHEN 1 THEN 2.0500
                    WHEN 2 THEN 12.4000 END::numeric(12,4)    AS crm_cert
    FROM generate_series(1, 100000) AS g(n)
) AS b
JOIN machine mc ON mc.machine_id = b.machine_id
CROSS JOIN (VALUES ('gold'), ('silver')) AS e(element)
CROSS JOIN LATERAL (
    SELECT CASE
        WHEN b.sample_type = 'crm' THEN
            round(( (CASE WHEN e.element = 'gold' THEN b.crm_cert
                          ELSE b.crm_cert * 18 END)
                    * (1 + (random() - 0.5) * 0.08) )::numeric, 4)
        ELSE
            -- log-normal: many low-grade samples, a few rich ones
            round(( (CASE WHEN e.element = 'gold' THEN 1 ELSE 18 END)
                    * exp((random() + random() + random() + random() - 2) * 2.0 - 0.7)
                  )::numeric, 4)
    END AS value_ppm
) AS v;

COMMIT;

ANALYZE machine;
ANALYZE assay_measurement;

-- What landed. Check these before running the full load: a short seed means
-- the snapshot will be short too, and nothing downstream will say so.
SELECT
  (SELECT COUNT(*) FROM machine)                            AS machines,
  (SELECT COUNT(*) FROM assay_measurement)                  AS measurements,
  (SELECT MIN(started_utc)::DATE FROM assay_measurement)    AS oldest_day,
  (SELECT MAX(started_utc)::DATE FROM assay_measurement)    AS newest_day,
  (SELECT COUNT(*) FROM assay_measurement
   WHERE sample_type = 'crm')                               AS crm_rows;
