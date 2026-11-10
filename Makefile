.PHONY: install data run lint docs clean

DAYS ?= 30
export DBT_PROFILES_DIR := $(CURDIR)/dbt/profiles
export DUCKDB_PATH := $(CURDIR)/data/warehouse.duckdb

install:            ## Install local dev dependencies
	pip install -r requirements-dev.txt

data:               ## Generate the synthetic landing zone only
	python generator/generate_data.py --days $(DAYS) --out data/landing

run:                ## Full local pipeline on DuckDB, one run per simulated day
	scripts/run_local_pipeline.sh $(DAYS)

lint:               ## Python + SQL lint
	ruff check . && ruff format --check .
	cd dbt && sqlfluff lint models tests

docs:               ## Generate and serve dbt docs (lineage)
	cd dbt && dbt docs generate && dbt docs serve --port 8081

clean:
	rm -rf data dbt/target dbt/logs
