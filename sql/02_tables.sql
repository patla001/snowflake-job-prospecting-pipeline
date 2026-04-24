-- ============================================================
-- 02_tables.sql — Table definitions for job prospecting
-- Run after 01_setup.sql (database JOB_PROSPECTING_DB must exist).
-- ============================================================
--
-- Fully qualified names; no USE DATABASE / USE SCHEMA required.
--
-- RAW schema: created here (idempotent) so this script still works if an older
-- 01_setup.sql was run before RAW existed. Use ACCOUNTADMIN (or another role
-- that may create schemas in this database); SYSADMIN alone often hits
-- "Schema ... does not exist or not authorized" when RAW was never created
-- or your role has no USAGE on it.
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- Database must exist (run 01_setup.sql first) before creating RAW.
CREATE SCHEMA IF NOT EXISTS JOB_PROSPECTING_DB.RAW
  COMMENT = 'Denormalized teaching tables (jobs, skills) for 02_tables.sql / 03_sample_data.sql';

-- SYSADMIN needs USAGE on the database (not only the schema) to resolve JOB_PROSPECTING_DB.RAW.*.
GRANT USAGE ON DATABASE JOB_PROSPECTING_DB TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA JOB_PROSPECTING_DB.RAW TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON SCHEMA JOB_PROSPECTING_DB.RAW TO ROLE SYSADMIN;

-- Tables created after this point (including in this script) inherit privileges for SYSADMIN.
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA JOB_PROSPECTING_DB.RAW TO ROLE SYSADMIN;

-- Skills dimension (e.g., Python, SQL, AWS)
CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.RAW.skills (
  skill_id    INTEGER AUTOINCREMENT PRIMARY KEY,
  skill_name  VARCHAR(100) NOT NULL,
  category    VARCHAR(50),   -- e.g., 'Programming', 'Data', 'Cloud'
  created_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  UNIQUE(skill_name)
);

-- Job postings fact table
CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.RAW.jobs (
  job_id          INTEGER AUTOINCREMENT PRIMARY KEY,
  title           VARCHAR(255) NOT NULL,
  company         VARCHAR(255),
  location        VARCHAR(255),
  location_type   VARCHAR(50),   -- 'Remote', 'Hybrid', 'On-site'
  salary_min      NUMBER(12, 2),
  salary_max      NUMBER(12, 2),
  salary_currency VARCHAR(3) DEFAULT 'USD',
  job_url         VARCHAR(2000),
  posted_date     DATE,
  source          VARCHAR(100),  -- e.g., 'LinkedIn', 'Indeed', 'Company Site'
  description     VARCHAR(16777216),
  created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Many-to-many: which skills are required for each job
CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.RAW.job_skills (
  job_id   INTEGER NOT NULL REFERENCES JOB_PROSPECTING_DB.RAW.jobs(job_id),
  skill_id INTEGER NOT NULL REFERENCES JOB_PROSPECTING_DB.RAW.skills(skill_id),
  is_required BOOLEAN DEFAULT TRUE,
  PRIMARY KEY (job_id, skill_id)
);

-- Note: CREATE INDEX applies only to Snowflake hybrid tables, not standard tables.
-- Filter performance on large tables: CLUSTER BY or search optimization in Snowflake docs.

-- Current tables in RAW (covers re-runs where CREATE TABLE IF NOT EXISTS is a no-op).
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA JOB_PROSPECTING_DB.RAW TO ROLE SYSADMIN;
