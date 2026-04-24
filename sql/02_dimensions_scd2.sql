-- ============================================================
-- 02_dimensions_scd2.sql — Dimension tables with SCD Type 2
-- Data modeling: star schema dimensions; history via effective dates
-- ============================================================
--
-- Prerequisite: sql/01_setup.sql (database JOB_PROSPECTING_DB and schema EDW exist).
--
-- CREATE TABLE uses three-part names (JOB_PROSPECTING_DB.EDW.<table>) so DDL works even
-- when this session has no "current database" (e.g. you run from an IDE, a partial
-- selection, or the UI context did not carry into the statement batch). Choosing
-- ACCOUNTADMIN and JOB_PROSPECTING_DB in the worksheet dropdown is not always enough.
--
-- Optional session context (Snowflake worksheet — run from the top when convenient):
-- ============================================================

USE WAREHOUSE JOB_PROSPECTING_WH;
USE DATABASE JOB_PROSPECTING_DB;
USE SCHEMA EDW;

-- Dim Company: SCD Type 2 — track company name/attribute changes over time
CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.EDW.dim_company (
  company_sk      INTEGER AUTOINCREMENT PRIMARY KEY,
  company_nk      VARCHAR(500) NOT NULL,   -- natural key (e.g., company name at first load)
  company_name    VARCHAR(500) NOT NULL,
  -- SCD2 columns
  effective_from  TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  effective_to    TIMESTAMP_NTZ,
  is_current      BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Dim Skill: SCD Type 2 — skill name or category changes
CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.EDW.dim_skill (
  skill_sk        INTEGER AUTOINCREMENT PRIMARY KEY,
  skill_nk        VARCHAR(200) NOT NULL,
  skill_name      VARCHAR(200) NOT NULL,
  category        VARCHAR(100),
  effective_from  TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  effective_to    TIMESTAMP_NTZ,
  is_current      BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Dim Location: SCD Type 2 — location label or type changes
CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.EDW.dim_location (
  location_sk     INTEGER AUTOINCREMENT PRIMARY KEY,
  location_nk     VARCHAR(500) NOT NULL,
  location_label  VARCHAR(500) NOT NULL,
  location_type   VARCHAR(50),
  city_region     VARCHAR(255),
  effective_from  TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  effective_to    TIMESTAMP_NTZ,
  is_current      BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Dim Source: Type 1 (no history) — source system rarely changes
CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.EDW.dim_source (
  source_sk       INTEGER AUTOINCREMENT PRIMARY KEY,
  source_nk      VARCHAR(100) NOT NULL UNIQUE,
  source_name    VARCHAR(200),
  created_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Dim Date: conformed dimension for reporting (no SCD)
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
