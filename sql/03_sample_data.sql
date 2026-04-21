-- ============================================================
-- 03_sample_data.sql — Seed sample data for job prospecting
-- Run after 02_tables.sql
-- ============================================================

USE DATABASE JOB_PROSPECTING_DB;
USE SCHEMA RAW;

-- Insert skills (merge so script is re-runnable)
MERGE INTO skills t
USING (
  SELECT * FROM VALUES
    ('Python', 'Programming'),
    ('SQL', 'Data'),
    ('Snowflake', 'Data'),
    ('AWS', 'Cloud'),
    ('Tableau', 'Analytics'),
    ('Excel', 'Analytics'),
    ('Spark', 'Data'),
    ('dbt', 'Data'),
    ('Machine Learning', 'Data'),
    ('ETL', 'Data')
  AS v(skill_name, category)
) s ON t.skill_name = s.skill_name
WHEN NOT MATCHED THEN INSERT (skill_name, category) VALUES (s.skill_name, s.category);

-- Insert sample job postings
INSERT INTO jobs (title, company, location, location_type, salary_min, salary_max, posted_date, source) VALUES
  ('Data Engineer', 'TechCorp Inc', 'San Diego, CA', 'Hybrid', 120000, 160000, '2025-02-01', 'LinkedIn'),
  ('Analytics Engineer', 'DataFlow LLC', 'Remote', 'Remote', 110000, 145000, '2025-02-05', 'Indeed'),
  ('Business Intelligence Analyst', 'RetailCo', 'San Diego, CA', 'On-site', 85000, 110000, '2025-02-10', 'Company Site'),
  ('Snowflake Developer', 'CloudFirst', 'Remote', 'Remote', 130000, 170000, '2025-02-12', 'LinkedIn'),
  ('Data Analyst', 'FinanceHub', 'San Diego, CA', 'Hybrid', 75000, 95000, '2025-02-15', 'Indeed');

-- Link jobs to skills (by title and skill name so IDs don't matter)
MERGE INTO job_skills t
USING (
  SELECT j.job_id, s.skill_id
  FROM jobs j
  CROSS JOIN skills s
  WHERE (j.title = 'Data Engineer'     AND s.skill_name IN ('Python', 'SQL', 'Snowflake', 'Spark', 'ETL'))
     OR (j.title = 'Analytics Engineer' AND s.skill_name IN ('SQL', 'dbt', 'Snowflake', 'Python'))
     OR (j.title = 'Business Intelligence Analyst' AND s.skill_name IN ('SQL', 'Tableau', 'Excel'))
     OR (j.title = 'Snowflake Developer' AND s.skill_name IN ('Snowflake', 'SQL', 'Python', 'ETL'))
     OR (j.title = 'Data Analyst' AND s.skill_name IN ('SQL', 'Excel', 'Tableau', 'Python'))
) s ON t.job_id = s.job_id AND t.skill_id = s.skill_id
WHEN NOT MATCHED THEN INSERT (job_id, skill_id) VALUES (s.job_id, s.skill_id);
