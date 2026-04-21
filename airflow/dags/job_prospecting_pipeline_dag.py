"""
Job Prospecting Pipeline DAG — Orchestrates Snowflake ingest, SCD2 merges, and fact load.
Requires: apache-airflow >= 2.5, apache-airflow-providers-snowflake (Snowflake connection type).
Uses SQLExecuteQueryOperator so stored procedures (CALL) run via the Snowflake connector, not the SQL API.
Connection: snowflake_default — Admin → Connections → Snowflake.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

SNOWFLAKE_CONN_ID = "snowflake_default"
DATABASE = Variable.get("snowflake_job_prospecting_database", default_var="JOB_PROSPECTING_DB")
SCHEMA_STAGING = Variable.get("snowflake_job_prospecting_schema_staging", default_var="STAGING")
SCHEMA_EDW = Variable.get("snowflake_job_prospecting_schema_edw", default_var="EDW")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="job_prospecting_pipeline",
    default_args=default_args,
    description="Snowflake job prospecting: ingest → staging merge → SCD2 → fact load",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["snowflake", "job-prospecting", "scd2"],
)


def _push_batch_id(**context):
    run_id = (context.get("run_id") or "manual")[:50]
    logical_date = context.get("logical_date")
    if logical_date:
        bid = logical_date.strftime("%Y%m%d_%H%M%S") + "_" + run_id.replace(":", "_").replace("+", "_")[-12:]
    else:
        bid = run_id.replace(":", "_").replace("+", "_") or "manual"
    context["ti"].xcom_push(key="batch_id", value=bid)
    return bid


task_get_batch_id = PythonOperator(
    task_id="get_batch_id",
    dag=dag,
    python_callable=_push_batch_id,
)

task_setup = SQLExecuteQueryOperator(
    task_id="setup_snowflake",
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql="""
    CREATE WAREHOUSE IF NOT EXISTS JOB_PROSPECTING_WH
      WITH WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE
      INITIALLY_SUSPENDED = TRUE COMMENT = 'Job prospecting capstone';
    CREATE DATABASE IF NOT EXISTS JOB_PROSPECTING_DB COMMENT = 'Job prospecting capstone';
    CREATE SCHEMA IF NOT EXISTS JOB_PROSPECTING_DB.STAGING COMMENT = 'Staging';
    CREATE SCHEMA IF NOT EXISTS JOB_PROSPECTING_DB.EDW COMMENT = 'EDW';
    CREATE SCHEMA IF NOT EXISTS JOB_PROSPECTING_DB.ANALYTICS COMMENT = 'Analytics';
    CREATE FILE FORMAT IF NOT EXISTS JOB_PROSPECTING_DB.STAGING.FF_CSV_JOBS
      TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('NULL', '') COMPRESSION = 'AUTO';
    CREATE STAGE IF NOT EXISTS JOB_PROSPECTING_DB.STAGING.STG_JOBS_FILES
      FILE_FORMAT = (FORMAT_NAME = 'JOB_PROSPECTING_DB.STAGING.FF_CSV_JOBS');
    """,
)

task_create_objects = SQLExecuteQueryOperator(
    task_id="create_objects",
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql="""
    USE DATABASE JOB_PROSPECTING_DB;
    CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.STAGING.stg_jobs_raw (
      ingest_batch_id VARCHAR(100) NOT NULL,
      ingest_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      file_name VARCHAR(500), row_number INTEGER,
      source_system VARCHAR(100) NOT NULL, external_job_id VARCHAR(255) NOT NULL,
      title VARCHAR(500), company_name VARCHAR(500), location_raw VARCHAR(500), location_type VARCHAR(50),
      salary_min NUMBER(12,2), salary_max NUMBER(12,2), salary_currency VARCHAR(3), job_url VARCHAR(2000),
      posted_date DATE, skills_raw VARCHAR(10000), source_updated_at TIMESTAMP_NTZ,
      PRIMARY KEY (source_system, external_job_id, ingest_batch_id)
    );
    CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.STAGING.stg_jobs_deduped (
      source_system VARCHAR(100) NOT NULL, external_job_id VARCHAR(255) NOT NULL,
      title VARCHAR(500), company_name VARCHAR(500), location_raw VARCHAR(500), location_type VARCHAR(50),
      salary_min NUMBER(12,2), salary_max NUMBER(12,2), salary_currency VARCHAR(3), job_url VARCHAR(2000),
      posted_date DATE, skills_raw VARCHAR(10000), source_updated_at TIMESTAMP_NTZ, ingest_batch_id VARCHAR(100) NOT NULL,
      PRIMARY KEY (source_system, external_job_id)
    );
    CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.EDW.dim_company (
      company_sk INTEGER AUTOINCREMENT PRIMARY KEY, company_nk VARCHAR(500) NOT NULL, company_name VARCHAR(500) NOT NULL,
      effective_from TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), effective_to TIMESTAMP_NTZ,
      is_current BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.EDW.dim_skill (
      skill_sk INTEGER AUTOINCREMENT PRIMARY KEY, skill_nk VARCHAR(200) NOT NULL, skill_name VARCHAR(200) NOT NULL,
      category VARCHAR(100), effective_from TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), effective_to TIMESTAMP_NTZ,
      is_current BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.EDW.dim_location (
      location_sk INTEGER AUTOINCREMENT PRIMARY KEY, location_nk VARCHAR(500) NOT NULL, location_label VARCHAR(500) NOT NULL,
      location_type VARCHAR(50), city_region VARCHAR(255),
      effective_from TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), effective_to TIMESTAMP_NTZ,
      is_current BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.EDW.dim_source (
      source_sk INTEGER AUTOINCREMENT PRIMARY KEY, source_nk VARCHAR(100) NOT NULL UNIQUE, source_name VARCHAR(200),
      created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.EDW.dim_date (
      date_sk INTEGER NOT NULL PRIMARY KEY, full_date DATE NOT NULL,
      year SMALLINT, quarter SMALLINT, month SMALLINT, day_of_month SMALLINT,
      day_of_week SMALLINT, week_of_year SMALLINT, is_weekend BOOLEAN
    );
    CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.EDW.fact_job_posting (
      job_posting_sk INTEGER AUTOINCREMENT PRIMARY KEY, source_system VARCHAR(100) NOT NULL, external_job_id VARCHAR(255) NOT NULL,
      company_sk INTEGER NOT NULL, location_sk INTEGER NOT NULL, source_sk INTEGER NOT NULL, posted_date_sk INTEGER NOT NULL,
      salary_min NUMBER(12,2), salary_max NUMBER(12,2), salary_currency VARCHAR(3), job_url VARCHAR(2000), title_raw VARCHAR(500),
      effective_from TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), effective_to TIMESTAMP_NTZ,
      is_current BOOLEAN NOT NULL DEFAULT TRUE, ingest_batch_id VARCHAR(100),
      UNIQUE (source_system, external_job_id, effective_from)
    );
    CREATE TABLE IF NOT EXISTS JOB_PROSPECTING_DB.EDW.fact_job_skill (
      job_posting_sk INTEGER NOT NULL, skill_sk INTEGER NOT NULL, is_required BOOLEAN DEFAULT TRUE,
      effective_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), PRIMARY KEY (job_posting_sk, skill_sk)
    );
    """,
)

# Ingest: insert test rows with batch_id from Airflow XCom
task_ingest = SQLExecuteQueryOperator(
    task_id="ingest_to_staging",
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql="""
    INSERT INTO JOB_PROSPECTING_DB.STAGING.stg_jobs_raw (
      ingest_batch_id, source_system, external_job_id, title, company_name, location_raw,
      location_type, salary_min, salary_max, salary_currency, job_url, posted_date, skills_raw
    )
    SELECT
      '{{ ti.xcom_pull(task_ids="get_batch_id", key="batch_id") }}',
      'LINKEDIN',
      'ext_' || ROW_NUMBER() OVER (ORDER BY 1),
      v.title, v.company_name, v.location_raw, v.location_type,
      v.salary_min, v.salary_max, 'USD', NULL, v.posted_date, v.skills_raw
    FROM (SELECT * FROM VALUES
      ('Data Engineer', 'TechCorp Inc', 'San Diego, CA', 'Hybrid', 120000, 160000, DATEADD(day, -1, CURRENT_DATE()), 'Python,SQL,Snowflake'),
      ('Analytics Engineer', 'DataFlow LLC', 'Remote', 'Remote', 110000, 145000, DATEADD(day, -2, CURRENT_DATE()), 'SQL,dbt,Snowflake'),
      ('Data Analyst', 'FinanceHub', 'San Diego, CA', 'Hybrid', 75000, 95000, DATEADD(day, -3, CURRENT_DATE()), 'SQL,Excel,Tableau')
      AS v(title, company_name, location_raw, location_type, salary_min, salary_max, posted_date, skills_raw)
    ) v;
    """,
)

# Merge staging into deduped (requires procedures to exist — run sql/03_pipeline_*.sql in Snowflake first)
task_merge_staging = SQLExecuteQueryOperator(
    task_id="merge_staging_deduped",
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql="CALL JOB_PROSPECTING_DB.STAGING.merge_staging_deduped('{{ ti.xcom_pull(task_ids=\"get_batch_id\", key=\"batch_id\") }}');",
)

task_scd2_company = SQLExecuteQueryOperator(
    task_id="scd2_dim_company",
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql="CALL JOB_PROSPECTING_DB.EDW.merge_dim_company_scd2();",
)
task_scd2_location = SQLExecuteQueryOperator(
    task_id="scd2_dim_location",
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql="CALL JOB_PROSPECTING_DB.EDW.merge_dim_location_scd2();",
)
task_scd2_source = SQLExecuteQueryOperator(
    task_id="scd2_dim_source",
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql="CALL JOB_PROSPECTING_DB.EDW.merge_dim_source();",
)
task_scd2_skill = SQLExecuteQueryOperator(
    task_id="scd2_dim_skill",
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql="CALL JOB_PROSPECTING_DB.EDW.merge_dim_skill_scd2();",
)

task_load_fact = SQLExecuteQueryOperator(
    task_id="load_fact_job_posting",
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql="CALL JOB_PROSPECTING_DB.EDW.load_fact_job_posting('{{ ti.xcom_pull(task_ids=\"get_batch_id\", key=\"batch_id\") }}');",
)

# Task order: batch_id first, then setup → create_objects → ingest → merge → SCD2 (parallel) → fact
task_get_batch_id >> task_setup >> task_create_objects >> task_ingest >> task_merge_staging
task_merge_staging >> [task_scd2_company, task_scd2_location, task_scd2_source, task_scd2_skill] >> task_load_fact
