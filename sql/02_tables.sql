-- ============================================================
-- 02_tables.sql — Table definitions for job prospecting
-- Run after 01_setup.sql; use schema RAW
-- ============================================================

USE DATABASE JOB_PROSPECTING_DB;
USE SCHEMA RAW;

-- Skills dimension (e.g., Python, SQL, AWS)
CREATE TABLE IF NOT EXISTS skills (
  skill_id    INTEGER AUTOINCREMENT PRIMARY KEY,
  skill_name  VARCHAR(100) NOT NULL,
  category    VARCHAR(50),   -- e.g., 'Programming', 'Data', 'Cloud'
  created_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  UNIQUE(skill_name)
);

-- Job postings fact table
CREATE TABLE IF NOT EXISTS jobs (
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
CREATE TABLE IF NOT EXISTS job_skills (
  job_id   INTEGER NOT NULL REFERENCES jobs(job_id),
  skill_id INTEGER NOT NULL REFERENCES skills(skill_id),
  is_required BOOLEAN DEFAULT TRUE,
  PRIMARY KEY (job_id, skill_id)
);

-- Optional: index for common filters
CREATE INDEX IF NOT EXISTS idx_jobs_posted_date ON jobs(posted_date);
CREATE INDEX IF NOT EXISTS idx_jobs_location ON jobs(location);
CREATE INDEX IF NOT EXISTS idx_jobs_title ON jobs(title);
