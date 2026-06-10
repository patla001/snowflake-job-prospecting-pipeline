#!/usr/bin/env bash
# Wrapper that sources .env so SNOWSQL_PWD is available, then forwards all
# args to dbt with --profiles-dir/--project-dir pinned to ./dbt.
#
# Usage from repo root:
#   ./scripts/dbt.sh debug
#   ./scripts/dbt.sh deps
#   ./scripts/dbt.sh run --select dim_freeway
#   ./scripts/dbt.sh test --select dim_freeway
#
# Requires: a venv with dbt-core + dbt-snowflake installed and active
# (see dbt/README.md). The wrapper does not manage the venv — just creds.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

if [[ -f "${ENV_FILE}" ]]; then
  # Export every var defined in .env without echoing values.
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

if [[ -z "${SNOWSQL_PWD:-}" ]]; then
  echo "error: SNOWSQL_PWD not set." >&2
  echo "  Add a line to .env (gitignored):" >&2
  echo "    SNOWSQL_PWD=your-snowflake-password" >&2
  echo "  Or export it inline:  SNOWSQL_PWD='...' ./scripts/dbt.sh $*" >&2
  exit 1
fi

if ! command -v dbt >/dev/null 2>&1; then
  echo "error: dbt not on PATH. Activate the dbt venv first:" >&2
  echo "  source .venv-dbt/bin/activate" >&2
  echo "  (or:  python3 -m venv .venv-dbt && source .venv-dbt/bin/activate &&" >&2
  echo "        pip install 'dbt-core>=1.8,<2' 'dbt-snowflake>=1.8,<2')" >&2
  exit 1
fi

exec dbt "$@" \
  --profiles-dir "${REPO_ROOT}/dbt" \
  --project-dir  "${REPO_ROOT}/dbt"
