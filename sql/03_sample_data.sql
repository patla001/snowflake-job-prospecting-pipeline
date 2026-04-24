-- ============================================================
-- 03_sample_data.sql — Seed sample data for job prospecting
-- Run after sql/01_setup.sql and sql/02_tables.sql
-- ============================================================
--
-- All inserts run inside SP_LOAD_SAMPLE_DATA_RAW (one CALL). That avoids the
-- common “only the last SELECT ran” issue in Snowsight where counts stay 0.
--
-- First-time: run SECTION A (full file). After that you can run SECTION B only.
-- ============================================================

-- ========= SECTION A — environment (run once per account / after 01_setup) =========

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE JOB_PROSPECTING_WH;
ALTER WAREHOUSE JOB_PROSPECTING_WH RESUME IF SUSPENDED;

CREATE DATABASE IF NOT EXISTS JOB_PROSPECTING_DB
  COMMENT = 'Job prospecting capstone - dimensional model + SCD2';

CREATE SCHEMA IF NOT EXISTS JOB_PROSPECTING_DB.RAW
  COMMENT = 'Denormalized teaching tables (jobs, skills) for 02_tables.sql / 03_sample_data.sql';

GRANT USAGE ON DATABASE JOB_PROSPECTING_DB TO ROLE SYSADMIN;
GRANT ALL ON ALL SCHEMAS IN DATABASE JOB_PROSPECTING_DB TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE JOB_PROSPECTING_WH TO ROLE SYSADMIN;

-- ========= Load procedure (DDL — creates/replaces the procedure only) =========

CREATE OR REPLACE PROCEDURE JOB_PROSPECTING_DB.RAW.SP_LOAD_SAMPLE_DATA_RAW()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
  ALTER WAREHOUSE JOB_PROSPECTING_WH RESUME IF SUSPENDED;

  DELETE FROM JOB_PROSPECTING_DB.RAW.job_skills;
  DELETE FROM JOB_PROSPECTING_DB.RAW.jobs;
  DELETE FROM JOB_PROSPECTING_DB.RAW.skills;

  INSERT INTO JOB_PROSPECTING_DB.RAW.skills (skill_name, category) VALUES
    ('Python', 'Programming'),
    ('SQL', 'Data'),
    ('Snowflake', 'Data'),
    ('AWS', 'Cloud'),
    ('Tableau', 'Analytics'),
    ('Excel', 'Analytics'),
    ('Spark', 'Data'),
    ('dbt', 'Data'),
    ('Machine Learning', 'Data'),
    ('ETL', 'Data');

  INSERT INTO JOB_PROSPECTING_DB.RAW.jobs (title, company, location, location_type, salary_min, salary_max, posted_date, source) VALUES
    ('Data Engineer', 'TechCorp Inc', 'San Diego, CA', 'Hybrid', 120000, 160000, '2025-02-01', 'LinkedIn'),
    ('Analytics Engineer', 'DataFlow LLC', 'Remote', 'Remote', 110000, 145000, '2025-02-05', 'Indeed'),
    ('Business Intelligence Analyst', 'RetailCo', 'San Diego, CA', 'On-site', 85000, 110000, '2025-02-10', 'Company Site'),
    ('Snowflake Developer', 'CloudFirst', 'Remote', 'Remote', 130000, 170000, '2025-02-12', 'LinkedIn'),
    ('Data Analyst', 'FinanceHub', 'San Diego, CA', 'Hybrid', 75000, 95000, '2025-02-15', 'Indeed');

  INSERT INTO JOB_PROSPECTING_DB.RAW.job_skills (job_id, skill_id)
  SELECT DISTINCT j.job_id, s.skill_id
  FROM JOB_PROSPECTING_DB.RAW.jobs j
  INNER JOIN JOB_PROSPECTING_DB.RAW.skills s
    ON (
         (j.title = 'Data Engineer' AND s.skill_name IN ('Python', 'SQL', 'Snowflake', 'Spark', 'ETL'))
      OR (j.title = 'Analytics Engineer' AND s.skill_name IN ('SQL', 'dbt', 'Snowflake', 'Python'))
      OR (j.title = 'Business Intelligence Analyst' AND s.skill_name IN ('SQL', 'Tableau', 'Excel'))
      OR (j.title = 'Snowflake Developer' AND s.skill_name IN ('Snowflake', 'SQL', 'Python', 'ETL'))
      OR (j.title = 'Data Analyst' AND s.skill_name IN ('SQL', 'Excel', 'Tableau', 'Python'))
    );

  RETURN 'SP_LOAD_SAMPLE_DATA_RAW finished — run the verification SELECT below.';
END;
$$;

-- ========= SECTION B — load data (run this every time you need to refresh sample rows) =========

CALL JOB_PROSPECTING_DB.RAW.SP_LOAD_SAMPLE_DATA_RAW();

-- Verification (expected: skills 10, jobs 5, job_skills about 20)
SELECT 'skills' AS t, COUNT(*) AS n FROM JOB_PROSPECTING_DB.RAW.skills
UNION ALL
SELECT 'jobs', COUNT(*) FROM JOB_PROSPECTING_DB.RAW.jobs
UNION ALL
SELECT 'job_skills', COUNT(*) FROM JOB_PROSPECTING_DB.RAW.job_skills;
