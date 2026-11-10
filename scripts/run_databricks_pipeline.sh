#!/usr/bin/env bash
# Databricks end-to-end run, one orchestrated run per day (without Airflow):
#   bronze notebook submitted as a serverless job -> dbt build on the SQL warehouse
#
# Prerequisites: Databricks CLI configured (`databricks configure`), landing files
# uploaded to the volume, bronze notebook imported in /Users/<you>/01_bronze_ingestion,
# DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN exported for dbt.
#
# Usage: scripts/run_databricks_pipeline.sh 2026-09-01 7
set -euo pipefail
export MSYS_NO_PATHCONV=1  # Git Bash on Windows: keep /Users/... and /sql/... paths as is

START="${1:-2026-09-01}"
DAYS="${2:-7}"
CATALOG="${DATABRICKS_CATALOG:-stock_platform}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
command -v cygpath >/dev/null && ROOT="$(cygpath -m "$ROOT")"
export DBT_PROFILES_DIR="$ROOT/dbt/profiles" DBT_TARGET=databricks DATABRICKS_CATALOG="$CATALOG"

ME=$(databricks current-user me --output json | python -c "import json,sys; print(json.load(sys.stdin)['userName'])")
cd "$ROOT/dbt"
for i in $(seq 0 $((DAYS - 1))); do
  DATE=$(python -c "import datetime as d; print(d.date.fromisoformat('$START') + d.timedelta(days=$i))")
  echo "=== $DATE ==="
  cat > job.json <<JSON
{"run_name": "bronze_$DATE",
 "tasks": [{"task_key": "bronze",
            "notebook_task": {"notebook_path": "/Users/$ME/01_bronze_ingestion",
                              "base_parameters": {"catalog": "$CATALOG", "run_date": "$DATE"}}}]}
JSON
  databricks jobs submit --json @job.json > /dev/null
  # Grants on gold only if CONSUMER_GROUP is set (the group must exist in the workspace)
  dbt build --quiet --vars "{consumer_group: '${CONSUMER_GROUP:-}'}"
done
rm -f job.json
