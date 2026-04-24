-- ============================================================
-- 03_pipeline_fact_load.sql — Fact load from staging + dimension lookups
-- Data modeling: join staging to current dimension rows (SCD2), insert fact + bridge
-- ============================================================
--
-- Fully qualified procedure names; no USE DATABASE / USE SCHEMA required.
-- ============================================================

-- Fact load: one row per job from stg_jobs_deduped with surrogate keys from current dims
CREATE OR REPLACE PROCEDURE JOB_PROSPECTING_DB.EDW.load_fact_job_posting(batch_id VARCHAR)
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  BEGIN
    INSERT INTO fact_job_posting (
      source_system,
      external_job_id,
      company_sk,
      location_sk,
      source_sk,
      posted_date_sk,
      salary_min,
      salary_max,
      salary_currency,
      job_url,
      title_raw,
      effective_from,
      is_current,
      ingest_batch_id
    )
    SELECT
      s.source_system,
      s.external_job_id,
      c.company_sk,
      l.location_sk,
      sr.source_sk,
      COALESCE(d.date_sk, 19000101) AS posted_date_sk,
      s.salary_min,
      s.salary_max,
      COALESCE(s.salary_currency, 'USD'),
      s.job_url,
      s.title,
      CURRENT_TIMESTAMP(),
      TRUE,
      :batch_id
    FROM JOB_PROSPECTING_DB.STAGING.stg_jobs_deduped s
    JOIN dim_company c ON c.is_current AND TRIM(UPPER(c.company_nk)) = TRIM(UPPER(COALESCE(s.company_name, 'UNKNOWN')))
    JOIN dim_location l ON l.is_current AND TRIM(UPPER(l.location_nk)) = TRIM(UPPER(COALESCE(s.location_raw, 'Unknown')))
    JOIN dim_source sr ON sr.source_nk = s.source_system
    LEFT JOIN dim_date d ON d.full_date = s.posted_date
    WHERE s.ingest_batch_id = :batch_id
      AND NOT EXISTS (
        SELECT 1 FROM fact_job_posting f
        WHERE f.source_system = s.source_system AND f.external_job_id = s.external_job_id AND f.is_current
      );
    RETURN 'fact_job_posting load done for batch ' || :batch_id;
  END;
  $$;

-- Optional: close previous fact row when same natural key reappears (SCD2 on fact for job updates)
CREATE OR REPLACE PROCEDURE JOB_PROSPECTING_DB.EDW.close_outdated_fact_rows(batch_id VARCHAR)
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  DECLARE
    ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  BEGIN
    UPDATE fact_job_posting f
    SET effective_to = :ts, is_current = FALSE
    WHERE f.is_current
      AND (f.source_system, f.external_job_id) IN (
        SELECT source_system, external_job_id FROM JOB_PROSPECTING_DB.STAGING.stg_jobs_deduped WHERE ingest_batch_id = :batch_id
      );
    RETURN 'outdated fact rows closed';
  END;
  $$;
