-- ============================================================
-- 01_setup.sql — Snowflake Job Prospecting Capstone
-- Data engineering fundamentals: schemas, warehouse, stages for big data
-- Run this first in your Snowflake account
-- ============================================================
--
-- IMPORTANT: Run the ENTIRE script (select all → Run / Run All).
-- Running only the warehouse block leaves JOB_PROSPECTING_DB missing.
-- If CREATE DATABASE fails, use a role with that privilege (trial: USE ROLE ACCOUNTADMIN;).
--
-- Role tip: Objects created as ACCOUNTADMIN are NOT automatically visible to SYSADMIN.
-- If 02_staging.sql says "not authorized" after a successful 01, run 01 again as ACCOUNTADMIN
-- (the GRANT block below fixes that), OR run all scripts as ACCOUNTADMIN, OR set your
-- Airflow Snowflake connection to use the same role you used for CREATE DATABASE.
--
-- If CREATE DATABASE fails with "not authorized", your worksheet role cannot create
-- databases — use a role that can (often ACCOUNTADMIN on trial), or ask for USAGE on
-- an existing shared database and change object names in this repo to match.
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- 1) Database and schemas first (no warehouse required for this DDL)
CREATE DATABASE IF NOT EXISTS JOB_PROSPECTING_DB
  COMMENT = 'Job prospecting capstone - dimensional model + SCD2';

CREATE SCHEMA IF NOT EXISTS JOB_PROSPECTING_DB.RAW
  COMMENT = 'Denormalized teaching tables (jobs, skills) for 02_tables.sql / 03_sample_data.sql';

CREATE SCHEMA IF NOT EXISTS JOB_PROSPECTING_DB.STAGING
  COMMENT = 'Landing zone for raw job data; batch and incremental loads';

CREATE SCHEMA IF NOT EXISTS JOB_PROSPECTING_DB.EDW
  COMMENT = 'Enterprise data warehouse - star schema, SCD Type 2 dimensions';

CREATE SCHEMA IF NOT EXISTS JOB_PROSPECTING_DB.ANALYTICS
  COMMENT = 'Analytics views for job prospecting';

-- 2) Warehouse (OK if it already exists)
CREATE WAREHOUSE IF NOT EXISTS JOB_PROSPECTING_WH
  WITH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  SCALING_POLICY = 'STANDARD'
  COMMENT = 'Warehouse for job prospecting; scale up for large batch loads';

-- 3) File format and stage (depend on STAGING schema)
CREATE FILE FORMAT IF NOT EXISTS JOB_PROSPECTING_DB.STAGING.FF_CSV_JOBS
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', '')
  COMPRESSION = 'AUTO'
  COMMENT = 'CSV format for job posting files';

CREATE STAGE IF NOT EXISTS JOB_PROSPECTING_DB.STAGING.STG_JOBS_FILES
  FILE_FORMAT = (FORMAT_NAME = 'JOB_PROSPECTING_DB.STAGING.FF_CSV_JOBS')
  COMMENT = 'Stage for job posting CSV/JSON files - used by pipeline';

-- 4) Let SYSADMIN use this database (worksheets / roles often default to SYSADMIN after setup)
GRANT USAGE ON DATABASE JOB_PROSPECTING_DB TO ROLE SYSADMIN;
GRANT ALL ON ALL SCHEMAS IN DATABASE JOB_PROSPECTING_DB TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE JOB_PROSPECTING_WH TO ROLE SYSADMIN;

USE DATABASE JOB_PROSPECTING_DB;

-- Sanity check: you should see JOB_PROSPECTING_DB listed
SHOW DATABASES LIKE 'JOB_PROSPECTING_DB';
