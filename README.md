# Snowflake Job Prospecting — Capstone Project

A capstone project using **Snowflake** to analyze job market data for prospecting: skills in demand, salary trends, roles by location, and hiring patterns.

## Overview

- **Goal:** Build a data pipeline and analytics layer in Snowflake to support job prospecting (finding opportunities, understanding requirements, and market trends).
- **Stack:** Snowflake (warehouse, tables, views), SQL, optional Python for loading data.
- **Deliverables:** Database setup, sample job data model, analytical views, and example queries you can extend or present.
- **Pipeline execution:** See **[docs/PIPELINE_EXECUTION.md](docs/PIPELINE_EXECUTION.md)** for step-by-step instructions to run the pipeline in Snowflake and to orchestrate it with **Apache Airflow**.

## Project Structure

```
snowflake/
├── README.md
├── docs/
│   └── PIPELINE_EXECUTION.md # How to run the pipeline in Snowflake + Airflow
├── sql/
│   ├── 01_setup.sql          # Warehouse, database, schemas, stage
│   ├── 02_staging.sql        # Staging tables
│   ├── 02_dimensions_scd2.sql# Dimension tables (SCD Type 2)
│   ├── 02_fact.sql           # Fact table and bridge
│   ├── 03_pipeline_ingest.sql   # Ingest + merge_staging_deduped procedure
│   ├── 03_pipeline_scd2_merge.sql # SCD2 merge procedures
│   ├── 03_pipeline_fact_load.sql # Fact load procedures
│   └── 03_sample_data.sql   # Optional seed data
├── airflow/
│   └── dags/
│       └── job_prospecting_pipeline_dag.py  # Airflow DAG
├── requirements.txt
└── requirements-airflow.txt # Airflow + Snowflake provider
```

## Prerequisites

- Snowflake account (trial: https://signup.snowflake.com)
- Python 3.8+ (optional, for scripts and Airflow)
- Snowflake CLI or web UI to run SQL

## Quick Start

### 1. Snowflake setup

In Snowflake (Worksheets or SnowSQL), run in order:

```bash
# From project root, or copy-paste from each file:
# sql/01_setup.sql
# sql/02_tables.sql
# sql/03_sample_data.sql
# sql/04_views.sql
```

### 2. Load sample data (optional)

If you have a `data/sample_jobs.csv`:

```bash
pip install -r requirements.txt
# Set env vars: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
python scripts/load_sample_jobs.py
```

Or run `sql/03_sample_data.sql` to insert a small seed dataset directly.

### 3. Run analytics

Query the views in `sql/04_views.sql`, for example:

- `v_jobs_by_role` — job counts by job title
- `v_skills_in_demand` — most requested skills
- `v_salary_by_role_location` — salary stats by role and location
- `v_recent_postings` — latest postings for prospecting

## Data Model (Summary)

| Table       | Purpose |
|------------|---------|
| `jobs`     | Job postings (title, company, location, salary range, posted_date, etc.) |
| `job_skills`| Many-to-many: which skills are required per job |
| `skills`   | Skill dimension (name, category) |

You can extend with `companies`, `applications`, or external API feeds.

## Customization

- **Your own data:** Replace `data/sample_jobs.csv` with exported job data (e.g., from LinkedIn, Indeed, or course datasets) and adjust column names in `02_tables.sql` and `load_sample_jobs.py` if needed.
- **More tables:** Add `sql/02_tables.sql` DDL for companies, industries, or time-series tables.
- **Presentations:** Use the views and example queries in reports or slides; optionally connect BI tools (Tableau, Streamlit) to Snowflake.

## License

For academic use; adapt as needed for your course requirements.
