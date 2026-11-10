#!/usr/bin/env bash
# Local end-to-end run on DuckDB, simulating one orchestrated run per day:
#   landing partition -> bronze (append) -> dbt build (silver, snapshot, gold, tests)
# Running day by day is what lets the dbt snapshot build a real SCD2 history.
#
# Usage: scripts/run_local_pipeline.sh [DAYS] [START_DATE]
set -euo pipefail

DAYS="${1:-30}"
START="${2:-2026-09-01}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# Git Bash on Windows: convert /c/Users/... to C:/Users/... so that Python and dbt understand the paths
command -v cygpath >/dev/null && ROOT="$(cygpath -m "$ROOT")"
export DBT_PROFILES_DIR="$ROOT/dbt/profiles"
export DBT_TARGET=duckdb
export DUCKDB_PATH="$ROOT/data/warehouse.duckdb"

cd "$ROOT"
rm -rf data/landing data/warehouse.duckdb
python generator/generate_data.py --start "$START" --days "$DAYS" --out data/landing > /dev/null

cd dbt
[ "${SKIP_DEPS:-0}" = "1" ] || dbt deps --quiet
for partition in $(ls -d ../data/landing/dt=* | sort); do
  run_date="${partition##*dt=}"
  echo "=== run_date=$run_date ==="
  python ../scripts/load_bronze_local.py --landing ../data/landing --db "$DUCKDB_PATH" --date "$run_date" > /dev/null
  dbt build --quiet --warn-error-options '{"silence": ["NoNodesForSelectionCriteria"]}' || exit 1
done

dbt source freshness --quiet || true
echo "Done. Warehouse: $DUCKDB_PATH"
