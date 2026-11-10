# Astro project (Astronomer). The whole repository is the Astro project:
# dags/ for Airflow, dbt/ for the dbt project, generator/ and scripts/ for local mode.
FROM astrocrpublic.azurecr.io/runtime:3.3-9

# dbt lives in its own virtualenv to avoid dependency conflicts with Airflow
# (recommended setup for astronomer-cosmos).
RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir "dbt-core>=1.10,<2" "dbt-duckdb>=1.10,<2" "dbt-databricks>=1.10,<2" && \
    deactivate

# Install dbt packages at build time so tasks do not run `dbt deps` each time.
RUN cd dbt && ../dbt_venv/bin/dbt deps --profiles-dir profiles

ENV DBT_TARGET=duckdb \
    DUCKDB_PATH=/usr/local/airflow/include/data/warehouse.duckdb \
    LANDING_PATH=/usr/local/airflow/include/data/landing
