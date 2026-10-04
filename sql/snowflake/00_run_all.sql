-- 00_run_all.sql
-- Runs 01 to 07 in order.
--
-- EXECUTE IMMEDIATE FROM needs the files on a stage. Upload them once:
--   Snowsight: Data > Databases > CHRYSOS_POC > PUBLIC > Stages > SCRIPTS > + Files
--   or:        PUT file://sql/snowflake/0[1-7]_*.sql @public.scripts AUTO_COMPRESS=FALSE
--
-- They can also be opened and run individually in a worksheet, which is what
-- the first run should do: the LIST output in 03 is the only thing that tells
-- you whether S3 is reachable, and EXECUTE IMMEDIATE FROM hides it.

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE poc_wh;
USE DATABASE chrysos_poc;

CREATE STAGE IF NOT EXISTS public.scripts;

EXECUTE IMMEDIATE FROM @public.scripts/01_setup.sql;
EXECUTE IMMEDIATE FROM @public.scripts/02_integration.sql;
EXECUTE IMMEDIATE FROM @public.scripts/03_stage.sql;
EXECUTE IMMEDIATE FROM @public.scripts/04_bronze.sql;
EXECUTE IMMEDIATE FROM @public.scripts/05_silver.sql;
EXECUTE IMMEDIATE FROM @public.scripts/06_gold.sql;
EXECUTE IMMEDIATE FROM @public.scripts/07_q1_drift.sql;
