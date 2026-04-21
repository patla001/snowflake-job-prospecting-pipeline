# Pipeline Execution Guide — Job Prospecting on Snowflake

This document explains how to run the job prospecting pipeline in **Snowflake** (manually or via scripts) and how to orchestrate it with **Apache Airflow**.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Executing the Pipeline in Snowflake (Manual)](#executing-the-pipeline-in-snowflake-manual)
3. [Executing with Apache Airflow](#executing-with-apache-airflow)
4. [Troubleshooting](#troubleshooting)

---

## Prerequisites

- **Snowflake account** (trial: [signup.snowflake.com](https://signup.snowflake.com))
- **Snowflake UI** (Worksheets) or **SnowSQL** CLI
- For Airflow: **Python 3.9+**, **Apache Airflow 3.x**, and **Snowflake Airflow provider** (DAG uses `airflow.sdk` — see [Upgrading to Airflow 3](https://airflow.apache.org/docs/apache-airflow/3.2.0/installation/upgrading_to_airflow3.html))

---

## Executing the Pipeline in Snowflake (Manual)

Run the following in order. Use **Snowflake Worksheets** (web UI) or **SnowSQL** and execute each script in sequence.

### Phase 1: One-time setup

Run once per environment (or when adding new objects).

| Order | Script | Description |
|-------|--------|-------------|
| 1 | `sql/01_setup.sql` | Warehouse, database, schemas (STAGING, EDW, ANALYTICS), file format, stage |
| 2 | `sql/02_staging.sql` | Staging tables: `stg_jobs_raw`, `stg_jobs_deduped` |
| 3 | `sql/02_dimensions_scd2.sql` | Dimension tables with SCD2: `dim_company`, `dim_skill`, `dim_location`, `dim_source`, `dim_date` |
| 4 | `sql/02_fact.sql` | Fact table `fact_job_posting` and bridge `fact_job_skill` |

**Optional — date dimension:** If you have a script that populates `dim_date` (e.g. `03_seed_dim_date.sql`), run it after `02_dimensions_scd2.sql` so fact joins to `dim_date` work.

### Phase 2: Load data into staging

Choose one of the following.

**Option A: COPY INTO from stage (big data)**

1. Upload your CSV/JSON files to the Snowflake stage:
   - In Snowflake: **Databases → JOB_PROSPECTING_DB → STAGING → Stages → STG_JOBS_FILES**
   - Use **Upload** or put files there via SnowSQL/Snowflake CLI.

2. Set a **batch ID** for this run (e.g. UUID or timestamp):

   ```sql
   SET batch_id = (SELECT UUID_STRING());
   -- Or: SET batch_id = TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
   ```

3. Run a `COPY INTO` that matches your file layout. Example for a 14-column CSV (adjust column mapping to your file):

   ```sql
   USE DATABASE JOB_PROSPECTING_DB;
   USE SCHEMA STAGING;

   COPY INTO stg_jobs_raw (
     ingest_batch_id,
     file_name,
     row_number,
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
     skills_raw
   )
   FROM (
     SELECT
       $batch_id,
       METADATA$FILENAME,
       METADATA$FILE_ROW_NUMBER,
       $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
     FROM @STG_JOBS_FILES
   )
   FILE_FORMAT = (FORMAT_NAME = 'JOB_PROSPECTING_DB.STAGING.FF_CSV_JOBS')
   ON_ERROR = 'CONTINUE';
   ```

**Option B: Insert test data directly**

Run `sql/03_sample_data.sql` if it inserts into `stg_jobs_raw` or `stg_jobs_deduped`, and use the same `batch_id` value in the steps below where required.

### Phase 3: Run the pipeline (staging → EDW)

Use a **single batch ID** for the whole run (same value used in Phase 2, or a new one if you only populated `stg_jobs_deduped` directly).

1. **Merge staging into deduped table** (if you used Option A and copied into `stg_jobs_raw`):

   ```sql
   USE DATABASE JOB_PROSPECTING_DB;

   CALL STAGING.merge_staging_deduped('<your_batch_id>');
   ```

   Replace `<your_batch_id>` with the value of `batch_id` from Phase 2 (e.g. from `SELECT $batch_id;`).

2. **SCD2 and dimension merges:**

   ```sql
   CALL EDW.merge_dim_company_scd2();
   CALL EDW.merge_dim_location_scd2();
   CALL EDW.merge_dim_source();
   CALL EDW.merge_dim_skill_scd2();   -- optional; no-op if no skill parsing
   ```

3. **Load fact table:**

   ```sql
   CALL EDW.load_fact_job_posting('<your_batch_id>');
   ```

4. **(Optional) Close outdated fact rows** (if you support SCD on the fact):

   ```sql
   CALL EDW.close_outdated_fact_rows('<your_batch_id>');
   ```

### Summary: execution order (manual)

```
01_setup.sql → 02_staging.sql → 02_dimensions_scd2.sql → 02_fact.sql
    → [Optional: seed dim_date]
    → Load data (COPY INTO or 03_sample_data)
    → CALL STAGING.merge_staging_deduped(batch_id)
    → CALL EDW.merge_dim_company_scd2();
    → CALL EDW.merge_dim_location_scd2();
    → CALL EDW.merge_dim_source();
    → CALL EDW.load_fact_job_posting(batch_id);
```

---

## Executing with Apache Airflow

Airflow runs the same pipeline in order and passes a **batch ID** (e.g. from the DAG run) to the procedures that need it.

### 1. Install Airflow and Snowflake provider

From the project root:

```bash
pip install "apache-airflow>=2.5.0" "apache-airflow-providers-snowflake>=5.0.0"
```

Or use the project’s Airflow requirements file if present:

```bash
pip install -r requirements-airflow.txt
```

### 2. Configure Snowflake connection in Airflow

1. Open Airflow UI → **Admin → Connections**.
2. Add a new connection:
   - **Connection Id:** `snowflake_default` (or the ID used in the DAG).
   - **Connection Type:** `Snowflake`.
   - **Host:** `.<account_identifier>.snowflakecomputing.com` (e.g. `xy12345.us-east-1`).
   - **Login:** your Snowflake username.
   - **Password:** your Snowflake password.
   - **Schema:** `JOB_PROSPECTING_DB` (or leave blank and set in DAG/operator).
   - **Extra (JSON):**  
     `{"database": "JOB_PROSPECTING_DB", "warehouse": "JOB_PROSPECTING_WH", "role": "ACCOUNTADMIN"}`  
     Adjust `database`, `warehouse`, and `role` to your environment.

### 3. DAG location and schedule

- Place the DAG file in your Airflow **DAGs folder** (e.g. `~/airflow/dags/` or the path set in `airflow.cfg`).
- The project’s DAG file is: **`dags/job_prospecting_pipeline_dag.py`** (repo root — used by **Astronomer** and this repo’s `docker-compose`).  
  For a classic Airflow install, copy that file into your DAGs directory so Airflow can load it.

### 4. Deploy Snowflake procedures (required before first DAG run)

The DAG calls stored procedures in Snowflake. Create them once by running these scripts in Snowflake (in order):

- `sql/03_pipeline_ingest.sql` — creates `STAGING.merge_staging_deduped(batch_id)`
- `sql/03_pipeline_scd2_merge.sql` — creates `EDW.merge_dim_company_scd2()`, `merge_dim_location_scd2()`, `merge_dim_source()`, `merge_dim_skill_scd2()`
- `sql/03_pipeline_fact_load.sql` — creates `EDW.load_fact_job_posting(batch_id)`, `close_outdated_fact_rows(batch_id)`

After that, the Airflow DAG can run without re-running these scripts.

### 5. What the DAG does

The DAG defines tasks that:

1. **Setup (optional):** Run `01_setup.sql` (idempotent; safe to run every time or only on first deploy).
2. **Create objects:** Run `02_staging.sql`, `02_dimensions_scd2.sql`, `02_fact.sql` (idempotent).
3. **Ingest:** Run a SQL statement that either:
   - Runs `COPY INTO` from the stage (requires files in `@STG_JOBS_FILES`), or
   - Inserts a small set of test rows into `stg_jobs_raw` with the run’s batch ID.
4. **Merge staging:** `CALL STAGING.merge_staging_deduped(batch_id)`.
5. **SCD2 and dimensions:**  
   `CALL EDW.merge_dim_company_scd2();`  
   `CALL EDW.merge_dim_location_scd2();`  
   `CALL EDW.merge_dim_source();`  
   `CALL EDW.merge_dim_skill_scd2();`
6. **Fact load:** `CALL EDW.load_fact_job_posting(batch_id);`

The **batch ID** is derived from the Airflow run (e.g. `run_id` or `logical_date`) so each run has a unique identifier.

### 6. Run the DAG

- In the Airflow UI, find **job_prospecting_pipeline** (or the DAG id defined in the script).
- Unpause the DAG, then trigger a run (e.g. **Trigger DAG**).
- Monitor task success/failure in the Graph or Grid view.

### 7. Variables (optional)

You can use Airflow Variables to override defaults:

- `snowflake_job_prospecting_database` — e.g. `JOB_PROSPECTING_DB`
- `snowflake_job_prospecting_schema_staging` — e.g. `STAGING`
- `snowflake_job_prospecting_schema_edw` — e.g. `EDW`

Set them under **Admin → Variables** if your DAG reads them.

---

## Troubleshooting

| Issue | What to check |
|-------|----------------|
| Procedure not found | Use fully qualified names: `JOB_PROSPECTING_DB.STAGING.merge_staging_deduped(...)` and `JOB_PROSPECTING_DB.EDW.load_fact_job_posting(...)`. Ensure you ran `02_*` and `03_pipeline_*.sql` so procedures exist. |
| No rows in fact | Ensure staging has data for the batch_id you pass. Run `SELECT * FROM JOB_PROSPECTING_DB.STAGING.stg_jobs_deduped WHERE ingest_batch_id = '<batch_id>';`. Then check that dimension lookups (company_nk, location_nk, source_nk) match. |
| COPY INTO fails | Confirm file format (CSV columns, delimiter, header) matches `FF_CSV_JOBS` and column list. Check stage path and that files are present: `LIST @STG_JOBS_FILES;`. |
| Airflow Snowflake task fails | Verify connection (Host, account, user, password, warehouse). Ensure database/schema in connection Extra or in the operator match your Snowflake setup. Check task logs for the exact Snowflake error. |
| dim_date join returns 19000101 | Populate `dim_date` for the date range of your `posted_date` (e.g. run `03_seed_dim_date.sql` or equivalent). |

---

## Quick reference: procedure signatures

| Procedure | Schema | Arguments |
|-----------|--------|-----------|
| `merge_staging_deduped` | STAGING | `(batch_id VARCHAR)` |
| `merge_dim_company_scd2` | EDW | none |
| `merge_dim_location_scd2` | EDW | none |
| `merge_dim_source` | EDW | none |
| `merge_dim_skill_scd2` | EDW | none |
| `load_fact_job_posting` | EDW | `(batch_id VARCHAR)` |
| `close_outdated_fact_rows` | EDW | `(batch_id VARCHAR)` |

All of the above are in database `JOB_PROSPECTING_DB` (unless you changed it in setup).
