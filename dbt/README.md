# dbt project — pems_pipeline

A parallel transform layer being introduced alongside the existing Snowflake
Scripting procs. The procs (called by the Airflow DAG) still own production
data in the `EDW` schema; dbt builds run into a separate `DBT_MARTS` schema
so the two can be diffed safely.

## One-time setup

Use a **separate** Python venv from the Airflow one — `dbt-core` pins on
different dependency versions than Airflow 3.x and the two clobber each
other if installed in the same env.

```bash
python3 -m venv .venv-dbt
source .venv-dbt/bin/activate
pip install --upgrade pip
pip install 'dbt-core>=1.8,<2' 'dbt-snowflake>=1.8,<2'
```

## Configure credentials

Add one line to the gitignored `.env` at the repo root (the same file
that already holds `AIRFLOW_CONN_SNOWFLAKE_DEFAULT`):

```bash
SNOWSQL_PWD=your-snowflake-password
```

Use the **plain** password — not URL-encoded. `profiles.yml` reads it at
runtime via `{{ env_var('SNOWSQL_PWD') }}`, so nothing sensitive is ever
committed.

## Run from the repo root

Use the `scripts/dbt.sh` wrapper — it sources `.env` and pins
`--profiles-dir`/`--project-dir` so you don't have to.

```bash
./scripts/dbt.sh deps                              # install dbt-utils
./scripts/dbt.sh debug                             # "All checks passed!"
./scripts/dbt.sh run  --select dim_freeway         # build into DBT_MARTS
./scripts/dbt.sh test --select dim_freeway         # not_null tests
```

Equivalent raw form if you'd rather not use the wrapper:

```bash
export SNOWSQL_PWD='your-snowflake-password'
dbt debug --profiles-dir ./dbt --project-dir ./dbt
```

## Verify the port is correct

`dim_freeway` is a one-for-one port of `EDW.merge_dim_freeway()`. The
rowcounts should match exactly:

```sql
SELECT 'proc' AS source, COUNT(*) AS n FROM TRAFFIC_PEMS_DB.EDW.dim_freeway
UNION ALL
SELECT 'dbt',  COUNT(*) FROM TRAFFIC_PEMS_DB.DBT_MARTS.dim_freeway;
```

## What's here today

- `dim_freeway` — port of the simplest dim merge proc.

## What's coming (separate commits)

- Port `load_fact_traffic_hour` and `refresh_agg_traffic_daily` to incremental models.
- Replace the hand-rolled SCD2 in `merge_dim_station_scd2` with a `dbt snapshot`.
- Wire `dbt build` into the Airflow DAG, eventually replacing the proc-call tasks.
