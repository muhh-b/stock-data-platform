"""Daily stock data platform pipeline.

    check landing  ->  bronze ingestion  ->  source freshness  ->  dbt build (Cosmos)

* Local mode (DBT_TARGET=duckdb): bronze is loaded into DuckDB by a Python
  task, dbt runs on DuckDB. dbt tasks share a 1-slot pool because DuckDB
  accepts a single writer.
* Databricks mode (DBT_TARGET=databricks): bronze runs the PySpark notebook
  on Databricks (serverless job), dbt runs on the SQL warehouse through
  dbt-databricks.

Cosmos renders every dbt model, snapshot and test as its own Airflow task,
so a failure is visible (and retryable) at model level, and tests run right
after the model they check.

Local backfill over the generated period:
    astro dev run backfill create --dag-id stock_platform_daily --from-date 2026-09-01 --to-date 2026-09-30
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))
DBT_PROJECT_DIR = AIRFLOW_HOME / "dbt"
DBT_EXECUTABLE = AIRFLOW_HOME / "dbt_venv" / "bin" / "dbt"
TARGET = os.environ.get("DBT_TARGET", "duckdb")
LANDING_PATH = Path(os.environ.get("LANDING_PATH", AIRFLOW_HOME / "include" / "data" / "landing"))
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", str(AIRFLOW_HOME / "include" / "data" / "warehouse.duckdb"))

START_DATE = datetime(2026, 9, 1)
SIMULATED_DAYS = 60

profile_config = ProfileConfig(
    profile_name="stock_platform",
    target_name=TARGET,
    profiles_yml_filepath=DBT_PROJECT_DIR / "profiles" / "profiles.yml",
)

dbt_operator_args = {"install_deps": False}
if TARGET == "duckdb":
    dbt_operator_args["pool"] = "duckdb_single_writer"


@dag(
    dag_id="stock_platform_daily",
    description="Retail stock platform: bronze ingestion, then dbt silver / snapshot / gold with tests",
    schedule="@daily",
    start_date=START_DATE,
    end_date=START_DATE + timedelta(days=SIMULATED_DAYS - 1),
    catchup=False,
    max_active_runs=1,  # runs must be sequential: snapshots and incrementals depend on the previous day
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["stock", "dbt", "databricks"],
)
def stock_platform_daily():
    @task
    def check_landing(ds: str | None = None) -> str:
        """Local mode: generate the synthetic landing zone once, then check the day's partition."""
        partition = LANDING_PATH / f"dt={ds}"
        if TARGET == "duckdb" and not LANDING_PATH.exists():
            subprocess.run(
                [
                    sys.executable,
                    str(AIRFLOW_HOME / "generator" / "generate_data.py"),
                    "--start",
                    START_DATE.date().isoformat(),
                    "--days",
                    str(SIMULATED_DAYS),
                    "--out",
                    str(LANDING_PATH),
                ],
                check=True,
            )
        if TARGET == "duckdb" and not (partition / "stock.csv").exists():
            raise FileNotFoundError(f"Stock snapshot missing for {ds}: {partition}")
        return ds

    run_date = check_landing()

    if TARGET == "databricks":
        from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

        bronze = DatabricksSubmitRunOperator(
            task_id="bronze_ingestion",
            databricks_conn_id="databricks_default",
            # No cluster spec: the task runs on serverless jobs compute (Databricks Free Edition).
            json={
                "run_name": "stock_platform_bronze_{{ ds }}",
                "tasks": [
                    {
                        "task_key": "bronze_ingestion",
                        "notebook_task": {
                            "notebook_path": os.environ.get("DATABRICKS_BRONZE_NOTEBOOK", ""),
                            "base_parameters": {
                                "catalog": os.environ.get("DATABRICKS_CATALOG", "stock_platform"),
                                "run_date": "{{ ds }}",
                            },
                        },
                    }
                ],
            },
        )
    else:

        @task(task_id="bronze_ingestion", pool="duckdb_single_writer")
        def bronze_local(ds: str | None = None) -> None:
            subprocess.run(
                [
                    sys.executable,
                    str(AIRFLOW_HOME / "scripts" / "load_bronze_local.py"),
                    "--landing",
                    str(LANDING_PATH),
                    "--db",
                    DUCKDB_PATH,
                    "--date",
                    ds,
                ],
                check=True,
            )

        bronze = bronze_local()

    source_freshness = BashOperator(
        task_id="source_freshness",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && {DBT_EXECUTABLE} source freshness --profiles-dir profiles --target {TARGET}"
        ),
        pool="duckdb_single_writer" if TARGET == "duckdb" else "default_pool",
    )

    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=str(DBT_EXECUTABLE)),
        render_config=RenderConfig(test_behavior=TestBehavior.AFTER_EACH, dbt_deps=False),
        operator_args=dbt_operator_args,
    )

    run_date >> bronze >> source_freshness >> dbt_build


stock_platform_daily()
