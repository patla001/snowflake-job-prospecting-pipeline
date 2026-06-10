"""
PeMS dbt build DAG — runs `dbt snapshot` + `dbt build` against the same
Snowflake account as the proc-driven pems_traffic_pipeline DAG.

Why a separate DAG instead of a task in the existing one:
  - dbt-core's pinned deps clash with Airflow 3's task SDK if installed in
    the main Airflow venv (documented in CLAUDE.md). This DAG isolates dbt
    inside its own bootstrapped venv at /tmp/dbt_venv.
  - The proc-driven DAG continues to be production; this DAG demonstrates
    the dbt port path side-by-side, so both are visible in the UI.

Dependency on the proc DAG:
  - fact_traffic_hour and dim_station (dbt) read EDW.dim_station and
    EDW.dim_freeway as `source()`s, so the proc DAG must have run at least
    once against this account. After that, this DAG can run independently.

SNOWSQL_PWD is expected in the container env (docker-compose `env_file: .env`,
or `astro deployment variable create SNOWSQL_PWD=...` for Astro).
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="pems_dbt_build",
    default_args=default_args,
    description="dbt-snowflake snapshot + build + test for the PeMS marts (DBT_DEV_MARTS)",
    schedule=None,  # manual trigger; depends on pems_traffic_pipeline having run
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["snowflake", "pems", "dbt", "scd2"],
)

# Use the venv baked into the image at /opt/dbt-venv (Dockerfile/Dockerfile.local).
# Falls back to bootstrapping at /tmp/dbt_venv if the image wasn't rebuilt yet
# (e.g. running the DAG against an old image during dev).
_BOOTSTRAP = r"""
set -euo pipefail

DBT_VENV=/opt/dbt-venv
DBT_PROJECT="${AIRFLOW_HOME:-/opt/airflow}/dbt"

if [ ! -x "$DBT_VENV/bin/dbt" ]; then
  echo "WARN: /opt/dbt-venv not found — rebuild the Airflow image to pre-install."
  echo "Bootstrapping fallback venv at /tmp/dbt_venv (one-time, ~30 s)…"
  DBT_VENV=/tmp/dbt_venv
  python -m venv "$DBT_VENV"
  "$DBT_VENV/bin/pip" install --quiet --upgrade pip
  "$DBT_VENV/bin/pip" install --quiet 'dbt-snowflake>=1.8,<2'
fi

if [ -z "${SNOWSQL_PWD:-}" ]; then
  echo "error: SNOWSQL_PWD not set in the container env." >&2
  echo "  Local: set it in .env (docker-compose loads automatically)." >&2
  echo "  Astro: astro deployment variable create SNOWSQL_PWD=…" >&2
  exit 1
fi

cd "$DBT_PROJECT"
"$DBT_VENV/bin/dbt" deps     --profiles-dir . --project-dir .
"$DBT_VENV/bin/dbt" snapshot --profiles-dir . --project-dir .
"$DBT_VENV/bin/dbt" build    --profiles-dir . --project-dir .
"""

task_dbt_build = BashOperator(
    task_id="dbt_snapshot_and_build",
    bash_command=_BOOTSTRAP,
    append_env=True,
    dag=dag,
)
