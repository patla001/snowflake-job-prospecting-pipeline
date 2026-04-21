-- ============================================================
-- 03_pipeline_ingest.sql — Big data ingest: COPY INTO from stage
-- Data engineering: bulk load from files, batch id for idempotency
-- ============================================================

USE DATABASE JOB_PROSPECTING_DB;
USE SCHEMA STAGING;

-- Example: load CSV from internal stage into stg_jobs_raw
-- Replace column list and stage path with your file layout
-- Run with a unique ingest_batch_id per run (e.g., from task or pipeline tool)

-- Option A: COPY INTO with explicit columns and batch id (run from orchestration)
-- SET batch_id = (SELECT UUID_STRING());
-- COPY INTO stg_jobs_raw (
--   ingest_batch_id,
--   file_name,
--   row_number,
--   source_system,
--   external_job_id,
--   title,
--   company_name,
--   location_raw,
--   location_type,
--   salary_min,
--   salary_max,
--   salary_currency,
--   job_url,
--   posted_date,
--   skills_raw
-- )
-- FROM (
--   SELECT
--     $batch_id,
--     METADATA$FILENAME,
--     METADATA$FILE_ROW_NUMBER,
--     $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
--   FROM @STG_JOBS_FILES
-- )
-- FILE_FORMAT = (FORMAT_NAME = 'JOB_PROSPECTING_DB.STAGING.FF_CSV_JOBS')
-- ON_ERROR = 'CONTINUE';

-- Option B: Merge staging into deduped table (latest record per natural key per batch)
-- Call after COPY INTO to prepare one row per (source_system, external_job_id) for this batch
CREATE OR REPLACE PROCEDURE JOB_PROSPECTING_DB.STAGING.merge_staging_deduped(batch_id VARCHAR)
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  BEGIN
    MERGE INTO stg_jobs_deduped t
    USING (
      SELECT
        source_system,
        external_job_id,
        title,
        company_name,
        location_raw,
        location_type,
        salary_min,
        salary_max,
        salary_currency,
        job_url,
        posted_date,
        skills_raw,
        source_updated_at,
        ingest_batch_id
      FROM stg_jobs_raw
      WHERE ingest_batch_id = :batch_id
    ) s
    ON t.source_system = s.source_system AND t.external_job_id = s.external_job_id
    WHEN MATCHED AND t.ingest_batch_id < s.ingest_batch_id THEN UPDATE SET
      title = s.title,
      company_name = s.company_name,
      location_raw = s.location_raw,
      location_type = s.location_type,
      salary_min = s.salary_min,
      salary_max = s.salary_max,
      salary_currency = s.salary_currency,
      job_url = s.job_url,
      posted_date = s.posted_date,
      skills_raw = s.skills_raw,
      source_updated_at = s.source_updated_at,
      ingest_batch_id = s.ingest_batch_id
    WHEN NOT MATCHED THEN INSERT (
      source_system, external_job_id, title, company_name, location_raw,
      location_type, salary_min, salary_max, salary_currency, job_url,
      posted_date, skills_raw, source_updated_at, ingest_batch_id
    ) VALUES (
      s.source_system, s.external_job_id, s.title, s.company_name, s.location_raw,
      s.location_type, s.salary_min, s.salary_max, s.salary_currency, s.job_url,
      s.posted_date, s.skills_raw, s.source_updated_at, s.ingest_batch_id
    );
    RETURN 'OK: merged batch ' || :batch_id;
  END;
  $$;
