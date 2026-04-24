-- ============================================================
-- 03_seed_dim_date.sql — Populate EDW.dim_date (calendar + unknown sentinel)
-- Run after 02_dimensions_scd2.sql for a normal deploy; this file also creates dim_date
-- if it is missing so MERGE does not hit "Object 'DIM_DATE' does not exist or not authorized".
-- Idempotent: MERGE by date_sk; safe to re-run.
-- ============================================================
--
-- If you still see "not authorized", use the same Snowflake role that owns EDW (often
-- ACCOUNTADMIN in trial accounts), or ask for MERGE/INSERT/UPDATE on JOB_PROSPECTING_DB.EDW.dim_date.
-- ============================================================

USE WAREHOUSE JOB_PROSPECTING_WH;

CREATE SCHEMA IF NOT EXISTS JOB_PROSPECTING_DB.EDW
  COMMENT = 'Enterprise data warehouse - star schema, SCD Type 2 dimensions';

CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.EDW.dim_date (
  date_sk        INTEGER NOT NULL PRIMARY KEY,
  full_date      DATE NOT NULL,
  year           SMALLINT,
  quarter        SMALLINT,
  month          SMALLINT,
  day_of_month   SMALLINT,
  day_of_week    SMALLINT,
  week_of_year   SMALLINT,
  is_weekend     BOOLEAN
);

-- Sentinel row used when posted_date has no calendar match (keeps reporting joins clean)
MERGE INTO JOB_PROSPECTING_DB.EDW.dim_date t
USING (
  SELECT
    19000101 AS date_sk,
    DATE '1900-01-01' AS full_date,
    CAST(1900 AS SMALLINT) AS year,
    CAST(1 AS SMALLINT) AS quarter,
    CAST(1 AS SMALLINT) AS month,
    CAST(1 AS SMALLINT) AS day_of_month,
    CAST(1 AS SMALLINT) AS day_of_week,
    CAST(1 AS SMALLINT) AS week_of_year,
    FALSE AS is_weekend
) s
ON t.date_sk = s.date_sk
WHEN NOT MATCHED THEN
  INSERT (date_sk, full_date, year, quarter, month, day_of_month, day_of_week, week_of_year, is_weekend)
  VALUES (s.date_sk, s.full_date, s.year, s.quarter, s.month, s.day_of_month, s.day_of_week, s.week_of_year, s.is_weekend);

-- Calendar spine: adjust start/end if you need a wider reporting window
MERGE INTO JOB_PROSPECTING_DB.EDW.dim_date t
USING (
  SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD')) AS date_sk,
    d AS full_date,
    EXTRACT(YEAR FROM d)::SMALLINT AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT AS quarter,
    EXTRACT(MONTH FROM d)::SMALLINT AS month,
    EXTRACT(DAY FROM d)::SMALLINT AS day_of_month,
    DAYOFWEEK(d)::SMALLINT AS day_of_week,
    WEEKOFYEAR(d)::SMALLINT AS week_of_year,
    (DAYOFWEEK(d) IN (0, 6)) AS is_weekend
  FROM (
    SELECT DATEADD(day, SEQ4(), DATE '2018-01-01') AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))
  ) spine
  WHERE d <= DATE '2035-12-31'
) s
ON t.date_sk = s.date_sk
WHEN NOT MATCHED THEN
  INSERT (date_sk, full_date, year, quarter, month, day_of_month, day_of_week, week_of_year, is_weekend)
  VALUES (s.date_sk, s.full_date, s.year, s.quarter, s.month, s.day_of_month, s.day_of_week, s.week_of_year, s.is_weekend);
