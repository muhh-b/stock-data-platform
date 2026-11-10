"""Local bronze ingestion into DuckDB (mirror of the Databricks bronze notebook).

Reads the CSV files of one landing partition (``dt=YYYY-MM-DD``) and appends
them, as raw strings, to ``bronze.<entity>`` tables with technical columns:

* ``_ingested_at``    ingestion timestamp
* ``_source_file``    file the row comes from
* ``_partition_date`` landing partition date

Bronze is append-only on purpose: replaying a partition creates duplicates
that the silver layer removes. This is what makes the whole chain safe to
re-run.

Usage::

    python scripts/load_bronze_local.py --date 2026-09-01
    python scripts/load_bronze_local.py --all
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

ENTITIES = ["stores", "products", "stock", "sales", "receipts"]


def load_partition(con: duckdb.DuckDBPyConnection, partition: Path) -> None:
    partition_date = partition.name.removeprefix("dt=")
    for entity in ENTITIES:
        file = partition / f"{entity}.csv"
        if not file.exists():
            continue
        select = f"""
            select *,
                   current_timestamp::timestamp as _ingested_at,
                   '{file.as_posix()}'          as _source_file,
                   date '{partition_date}'      as _partition_date
            from read_csv('{file.as_posix()}', header = true, all_varchar = true, hive_partitioning = false)
        """
        exists = con.execute(
            "select count(*) from information_schema.tables where table_schema = 'bronze' and table_name = ?",
            [entity],
        ).fetchone()[0]
        if exists:
            con.execute(f"insert into bronze.{entity} by name {select}")
        else:
            con.execute(f"create table bronze.{entity} as {select}")
        rows = con.execute(
            f"select count(*) from read_csv('{file.as_posix()}', header = true, hive_partitioning = false)"
        ).fetchone()[0]
        print(f"  bronze.{entity:<9} +{rows} rows from {file}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--landing", type=Path, default=Path("data/landing"))
    parser.add_argument("--db", type=Path, default=Path("data/warehouse.duckdb"))
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", help="landing partition to load (YYYY-MM-DD)")
    group.add_argument("--all", action="store_true", help="load every partition")
    args = parser.parse_args()

    args.db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(args.db))
    con.execute("create schema if not exists bronze")

    partitions = sorted(args.landing.glob("dt=*")) if args.all else [args.landing / f"dt={args.date}"]
    for partition in partitions:
        if not partition.exists():
            raise SystemExit(f"Partition not found: {partition}")
        print(f"Loading {partition.name}")
        load_partition(con, partition)
    con.close()


if __name__ == "__main__":
    main()
