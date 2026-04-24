-- ============================================================
-- 02_fact.sql — Fact table and bridge for star schema
-- Data modeling: fact_job_posting links to SCD2 dimensions via surrogate keys
-- ============================================================
--
-- Fully qualified names — no USE DATABASE / USE SCHEMA required.
-- Run after 02_dimensions_scd2.sql (FKs reference dim_skill).
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- Fact: one row per job posting (grain: one posting per source + external_id)
-- Surrogate keys point to current (or point-in-time) dimension versions
CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.EDW.fact_job_posting (
  job_posting_sk   INTEGER AUTOINCREMENT PRIMARY KEY,
  -- Natural key for idempotency and incremental
  source_system   VARCHAR(100) NOT NULL,
  external_job_id VARCHAR(255) NOT NULL,
  -- Foreign keys to dimensions (SCD2: use _sk from dims)
  company_sk      INTEGER NOT NULL,
  location_sk     INTEGER NOT NULL,
  source_sk       INTEGER NOT NULL,
  posted_date_sk  INTEGER NOT NULL,
  -- Measures and degenerate dimensions
  salary_min      NUMBER(12, 2),
  salary_max      NUMBER(12, 2),
  salary_currency VARCHAR(3),
  job_url         VARCHAR(2000),
  title_raw       VARCHAR(500),
  -- Pipeline metadata
  effective_from  TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  effective_to    TIMESTAMP_NTZ,
  is_current      BOOLEAN NOT NULL DEFAULT TRUE,
  ingest_batch_id VARCHAR(100),
  UNIQUE (source_system, external_job_id, effective_from)
);

-- Clustering for big data: time-series and filter by date/source
ALTER TABLE JOB_PROSPECTING_DB.EDW.fact_job_posting CLUSTER BY (posted_date_sk, source_sk);

-- Bridge: many-to-many between job posting and skills (fact grain + skill)
CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.EDW.fact_job_skill (
  job_posting_sk   INTEGER NOT NULL,
  skill_sk         INTEGER NOT NULL,
  is_required      BOOLEAN DEFAULT TRUE,
  effective_from   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (job_posting_sk, skill_sk),
  FOREIGN KEY (job_posting_sk) REFERENCES JOB_PROSPECTING_DB.EDW.fact_job_posting(job_posting_sk),
  FOREIGN KEY (skill_sk)       REFERENCES JOB_PROSPECTING_DB.EDW.dim_skill(skill_sk)
);
