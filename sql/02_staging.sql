-- ============================================================
-- 02_staging.sql — Staging (landing) layer for big data ingest
-- Data engineering: raw landing table with batch metadata for idempotency
-- ============================================================
--
-- PREREQUISITES
-- 1) Run sql/01_setup.sql first (recommended). This file can still bootstrap DB + STAGING alone.
-- 2) Picking a database in the Snowflake UI sidebar does NOT always set SQL session context.
--    We avoid USE DATABASE here so DDL uses only fully-qualified names (no session DB required).
-- 3) Use a role that can create databases/schemas/tables (trial: ACCOUNTADMIN).
-- ============================================================

-- Make the active role explicit (worksheets often default to SYSADMIN).
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS JOB_PROSPECTING_DB
  COMMENT = 'Job prospecting capstone - dimensional model + SCD2';

CREATE SCHEMA IF NOT EXISTS JOB_PROSPECTING_DB.STAGING
  COMMENT = 'Landing zone for raw job data; batch and incremental loads';

-- Raw landing table: one row per job posting as ingested from files
-- Supports big data: high volume, partition-friendly (ingest_batch_id, ingest_ts)
-- Natural key: source + external_id for deduplication and incremental loads
CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.STAGING.stg_jobs_raw (
  -- Batch / pipeline metadata (big data: partition and reprocess by batch)
  ingest_batch_id   VARCHAR(100) NOT NULL,
  ingest_ts         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  file_name         VARCHAR(500),
  row_number        INTEGER,
  -- Natural key for upsert and SCD source
  source_system     VARCHAR(100) NOT NULL,
  external_job_id   VARCHAR(255) NOT NULL,
  -- Business attributes (as landed)
  title             VARCHAR(500),
  company_name      VARCHAR(500),
  location_raw      VARCHAR(500),
  location_type     VARCHAR(50),
  salary_min        NUMBER(12, 2),
  salary_max        NUMBER(12, 2),
  salary_currency   VARCHAR(3),
  job_url           VARCHAR(2000),
  posted_date       DATE,
  description       VARCHAR(16777216),
  skills_raw        VARCHAR(10000),   -- comma-separated or JSON for parsing
  -- Dedupe and incremental
  source_updated_at TIMESTAMP_NTZ,
  PRIMARY KEY (source_system, external_job_id, ingest_batch_id)
);

-- Clustering for big data: range queries and incremental by batch/date
ALTER TABLE JOB_PROSPECTING_DB.STAGING.stg_jobs_raw CLUSTER BY (ingest_batch_id, posted_date);

-- Staging for dimension lookups during SCD2: current snapshot of staging for this batch
CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.STAGING.stg_jobs_deduped (
  source_system     VARCHAR(100) NOT NULL,
  external_job_id   VARCHAR(255) NOT NULL,
  title             VARCHAR(500),
  company_name      VARCHAR(500),
  location_raw      VARCHAR(500),
  location_type     VARCHAR(50),
  salary_min        NUMBER(12, 2),
  salary_max        NUMBER(12, 2),
  salary_currency   VARCHAR(3),
  job_url           VARCHAR(2000),
  posted_date       DATE,
  skills_raw        VARCHAR(10000),
  source_updated_at TIMESTAMP_NTZ,
  ingest_batch_id   VARCHAR(100) NOT NULL,
  PRIMARY KEY (source_system, external_job_id)
);
