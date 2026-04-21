-- ============================================================
-- 01_setup.sql — Snowflake Job Prospecting Capstone
-- Data engineering fundamentals: schemas, warehouse, stages for big data
-- Run this first in your Snowflake account
-- ============================================================

-- Warehouse: use X-SMALL for dev; scale to MEDIUM/LARGE for big data loads
CREATE WAREHOUSE IF NOT EXISTS JOB_PROSPECTING_WH
  WITH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  SCALING_POLICY = 'STANDARD'
  COMMENT = 'Warehouse for job prospecting; scale up for large batch loads';

CREATE DATABASE IF NOT EXISTS JOB_PROSPECTING_DB
  COMMENT = 'Job prospecting capstone - dimensional model + SCD2';

-- Staging: raw landing from files (big data ingest)
CREATE SCHEMA IF NOT EXISTS JOB_PROSPECTING_DB.STAGING
  COMMENT = 'Landing zone for raw job data; batch and incremental loads';

-- EDW: dimensional model (dimensions with SCD2 + fact tables)
CREATE SCHEMA IF NOT EXISTS JOB_PROSPECTING_DB.EDW
  COMMENT = 'Enterprise data warehouse - star schema, SCD Type 2 dimensions';

-- Analytics: views and reporting
CREATE SCHEMA IF NOT EXISTS JOB_PROSPECTING_DB.ANALYTICS
  COMMENT = 'Analytics views for job prospecting';

-- File format for CSV/JSON landing (big data: compressed, optional strip_outer_array for JSON)
CREATE FILE FORMAT IF NOT EXISTS JOB_PROSPECTING_DB.STAGING.FF_CSV_JOBS
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', '')
  COMPRESSION = 'AUTO'
  COMMENT = 'CSV format for job posting files';

-- Internal stage for bulk load (big data pattern: load files here, then COPY INTO)
CREATE STAGE IF NOT EXISTS JOB_PROSPECTING_DB.STAGING.STG_JOBS_FILES
  FILE_FORMAT = (FORMAT_NAME = 'JOB_PROSPECTING_DB.STAGING.FF_CSV_JOBS')
  COMMENT = 'Stage for job posting CSV/JSON files - used by pipeline';

USE DATABASE JOB_PROSPECTING_DB;
