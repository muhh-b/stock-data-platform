# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion
# MAGIC
# MAGIC Reads the raw CSV files of one landing partition (`dt=YYYY-MM-DD`) from a
# MAGIC Unity Catalog Volume and appends them to `bronze.<entity>` Delta tables.
# MAGIC
# MAGIC * Every column is kept as a **string**: typing happens in the silver layer (dbt).
# MAGIC * Technical columns are added: `_ingested_at`, `_source_file`, `_partition_date`.
# MAGIC * Bronze is **append-only**. Replaying a partition creates duplicates that silver
# MAGIC   removes, so the whole chain stays idempotent.
# MAGIC
# MAGIC Parameters (job / Airflow): `catalog`, `run_date`.

# COMMAND ----------

dbutils.widgets.text("catalog", "stock_platform")
dbutils.widgets.text("run_date", "2026-09-01")

catalog = dbutils.widgets.get("catalog")
run_date = dbutils.widgets.get("run_date")
landing_root = f"/Volumes/{catalog}/bronze/landing"
partition_path = f"{landing_root}/dt={run_date}"

ENTITIES = ["stores", "products", "stock", "sales", "receipts"]

print(f"catalog={catalog} run_date={run_date} partition={partition_path}")

# COMMAND ----------

from pyspark.sql import functions as F


def file_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:  # noqa: BLE001 - dbutils raises a generic exception when the path is missing
        return False


def ingest(entity: str) -> int:
    path = f"{partition_path}/{entity}.csv"
    if not file_exists(path):
        print(f"skip {entity}: no file for {run_date}")
        return 0

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")  # raw strings, typing is done in silver
        .csv(path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_partition_date", F.lit(run_date).cast("date"))
    )
    (df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.bronze.{entity}"))
    rows = df.count()
    print(f"bronze.{entity}: +{rows} rows")
    return rows


results = {entity: ingest(entity) for entity in ENTITIES}

# COMMAND ----------

# Fail the task if the mandatory daily stock snapshot is missing, so that the
# orchestrator stops before dbt builds KPIs on an incomplete day.
if results["stock"] == 0:
    raise ValueError(f"No stock snapshot landed for {run_date}")

dbutils.notebook.exit(str(results))
