# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 – Auto Loader: `landing/wiki/` → bronze
# MAGIC **Covers 2.3 (Auto Loader, schema enforcement/evolution, batch mode, file detection) · 2.1 (incremental) · 2.7 (JSON as VARIANT)**

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "appletvshow"
WIKI = f"/Volumes/{CATALOG}/bronze/landing/wiki"
CKPT = f"/Volumes/{CATALOG}/bronze/checkpoints/wiki"

# COMMAND ----------

# MAGIC %md
# MAGIC ### A. Show records → `bronze.wiki_current_programming`  **[2.3 + 2.1 incremental]**
# MAGIC - **Incremental:** the checkpoint (RocksDB) remembers processed files, so each run loads only new ones
# MAGIC   ([Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)).
# MAGIC - **Batch mode:** `availableNow` processes what's there, then stops. `Trigger.Once` is deprecated
# MAGIC   ([triggers](https://docs.databricks.com/aws/en/structured-streaming/triggers)).
# MAGIC - **Schema inference:** stored under `schemaLocation`. JSON columns infer as strings by default.
# MAGIC - **Schema evolution `addNewColumns`:** a new Wikipedia column fails the stream once; the rerun adds it.
# MAGIC   Use `rescue` instead to keep running and park unexpected data in `_rescued_data`
# MAGIC   ([schema](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)).
# MAGIC - **File detection:** directory listing is the default. File notification with file events is recommended
# MAGIC   at scale on external locations
# MAGIC   ([detection modes](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/file-detection-modes)).

# COMMAND ----------

(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CKPT}/records/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(f"{WIKI}/records/")
    .select("*",
            F.lit("wiki").alias("_source_system"),
            F.current_timestamp().alias("_ingest_ts"),
            F.col("_metadata.file_path").alias("_source_file"))
 .writeStream
    .option("checkpointLocation", f"{CKPT}/records/cp")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.bronze.wiki_current_programming")
 .awaitTermination())

# COMMAND ----------

# MAGIC %md
# MAGIC ### B. Raw API JSON → `bronze.wiki_api_raw` as VARIANT  **[2.7 semi-structured / nested JSON]**
# MAGIC `singleVariantColumn` lands each whole JSON record in one VARIANT column, so source schema changes never break bronze.
# MAGIC Trade-offs: no schema evolution, no `_rescued_data`, and records must be 16 MB or smaller
# MAGIC ([ingest as VARIANT](https://docs.databricks.com/aws/en/ingestion/variant)).

# COMMAND ----------

(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("singleVariantColumn", "payload")
    .load(f"{WIKI}/api_json/")
    .select("*",
            F.lit("wiki_api").alias("_source_system"),
            F.current_timestamp().alias("_ingest_ts"),
            F.col("_metadata.file_path").alias("_source_file"))
 .writeStream
    .option("checkpointLocation", f"{CKPT}/api_json/cp")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.bronze.wiki_api_raw")
 .awaitTermination())

# COMMAND ----------

# Query nested fields with ':' path syntax, and explode the sections array
display(spark.sql(f"""
  SELECT payload:revid::bigint AS revid,
         s.value:line::string  AS section,
         s.value:level::int    AS level
  FROM {CATALOG}.bronze.wiki_api_raw,
       LATERAL variant_explode(payload:sections) AS s
  LIMIT 50"""))