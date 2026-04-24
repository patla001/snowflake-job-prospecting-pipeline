-- ============================================================
-- 03_pipeline_scd2_merge.sql — Slowly Changing Dimension Type 2
-- Data engineering: close current row, insert new row when attributes change
-- ============================================================
--
-- Fully qualified procedure names; USE DATABASE after bootstrap sets session context.
-- Prerequisite: dimension tables in JOB_PROSPECTING_DB.EDW (see 02_dimensions_scd2.sql).
--
-- Bootstrap below matches 01_setup.sql so this script does not fail if the database was
-- never created. If you see "not authorized", use the same role as for 01_setup / ask for
-- USAGE on JOB_PROSPECTING_DB.
-- ============================================================

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS JOB_PROSPECTING_DB
  COMMENT = 'Job prospecting capstone - dimensional model + SCD2';

CREATE SCHEMA IF NOT EXISTS JOB_PROSPECTING_DB.EDW
  COMMENT = 'Enterprise data warehouse - star schema, SCD Type 2 dimensions';

CREATE SCHEMA IF NOT EXISTS JOB_PROSPECTING_DB.STAGING
  COMMENT = 'Landing zone for raw job data; batch and incremental loads';

USE DATABASE JOB_PROSPECTING_DB;

-- SCD2 merge for dim_company (natural key = company_name for simplicity; can use company_id if available)
CREATE OR REPLACE PROCEDURE JOB_PROSPECTING_DB.EDW.merge_dim_company_scd2()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  DECLARE
    ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  BEGIN
    -- Close current rows where attributes changed (match on natural key, compare attributes)
    UPDATE dim_company t
    SET effective_to = :ts, is_current = FALSE
    WHERE t.is_current
      AND EXISTS (
        SELECT 1 FROM JOB_PROSPECTING_DB.STAGING.stg_jobs_deduped s
        WHERE TRIM(UPPER(s.company_name)) = TRIM(UPPER(t.company_name))
          AND (t.company_name <> s.company_name OR (s.company_name IS NULL AND t.company_name IS NOT NULL) OR (s.company_name IS NOT NULL AND t.company_name IS NULL))
      );
    -- Insert new versions: new natural keys OR changed attributes (handled by insert below for new rows)
    INSERT INTO dim_company (company_nk, company_name, effective_from, effective_to, is_current)
    SELECT
      COALESCE(TRIM(s.company_name), 'UNKNOWN') AS company_nk,
      COALESCE(TRIM(s.company_name), 'UNKNOWN') AS company_name,
      :ts AS effective_from,
      NULL AS effective_to,
      TRUE AS is_current
    FROM JOB_PROSPECTING_DB.STAGING.stg_jobs_deduped s
    WHERE NOT EXISTS (
      SELECT 1 FROM dim_company d
      WHERE d.is_current AND TRIM(UPPER(d.company_nk)) = TRIM(UPPER(COALESCE(s.company_name, 'UNKNOWN')))
    )
    GROUP BY company_nk, company_name;
    RETURN 'dim_company SCD2 merge done';
  END;
  $$;

-- SCD2 merge for dim_skill (natural key = skill_name)
CREATE OR REPLACE PROCEDURE JOB_PROSPECTING_DB.EDW.merge_dim_skill_scd2()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  DECLARE
    ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  BEGIN
    -- Skills are usually Type 1 in practice; here we implement Type 2 for learning.
    -- Insert new skills from staging (parsed from skills_raw in a separate step; here we rely on existing dim_skill or a seed).
    -- This procedure can be called after parsing skills_raw into a temp table of skill names.
    RETURN 'dim_skill SCD2 merge (no-op unless skill source from staging)';
  END;
  $$;

-- SCD2 merge for dim_location (natural key = location_label + location_type)
CREATE OR REPLACE PROCEDURE JOB_PROSPECTING_DB.EDW.merge_dim_location_scd2()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  DECLARE
    ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  BEGIN
    UPDATE dim_location t
    SET effective_to = :ts, is_current = FALSE
    WHERE t.is_current
      AND EXISTS (
        SELECT 1 FROM JOB_PROSPECTING_DB.STAGING.stg_jobs_deduped s
        WHERE TRIM(UPPER(COALESCE(s.location_raw, ''))) = TRIM(UPPER(t.location_label))
          AND (t.location_label <> COALESCE(s.location_raw, '') OR t.location_type <> COALESCE(s.location_type, ''))
      );
    INSERT INTO dim_location (location_nk, location_label, location_type, city_region, effective_from, effective_to, is_current)
    SELECT
      COALESCE(TRIM(s.location_raw), 'Unknown') AS location_nk,
      COALESCE(TRIM(s.location_raw), 'Unknown') AS location_label,
      COALESCE(TRIM(s.location_type), 'Unknown') AS location_type,
      COALESCE(TRIM(s.location_raw), 'Unknown') AS city_region,
      :ts AS effective_from,
      NULL AS effective_to,
      TRUE AS is_current
    FROM JOB_PROSPECTING_DB.STAGING.stg_jobs_deduped s
    WHERE NOT EXISTS (
      SELECT 1 FROM dim_location d
      WHERE d.is_current
        AND TRIM(UPPER(d.location_nk)) = TRIM(UPPER(COALESCE(s.location_raw, 'Unknown')))
    )
    GROUP BY location_nk, location_label, location_type, city_region;
    RETURN 'dim_location SCD2 merge done';
  END;
  $$;

-- Type 1 (no history) for dim_source
CREATE OR REPLACE PROCEDURE JOB_PROSPECTING_DB.EDW.merge_dim_source()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  BEGIN
    INSERT INTO dim_source (source_nk, source_name)
    SELECT DISTINCT s.source_system, s.source_system
    FROM JOB_PROSPECTING_DB.STAGING.stg_jobs_deduped s
    WHERE NOT EXISTS (SELECT 1 FROM dim_source d WHERE d.source_nk = s.source_system);
    RETURN 'dim_source merge done';
  END;
  $$;

-- Role grants: use 01_setup.sql (avoids duplicate GRANT worksheet warnings).
