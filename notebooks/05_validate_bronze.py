# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05 – Validate `appletvshow.bronze`
# MAGIC Final task of the Lakeflow Job. Every load should be traceable, nothing duplicated, and schema drift caught rather than lost.

# COMMAND ----------

CATALOG = "appletvshow"
tables = {
    "manual_apple_tv_show": "manual_load",
    "wiki_current_programming": "wiki",
    "wiki_api_raw": "wiki_api",
    "gdrive_mridul": "google_drive",
}

existing = {r.tableName for r in spark.sql(f"SHOW TABLES IN {CATALOG}.bronze").collect()}
rows = []
for t in tables:
    if t not in existing:
        rows.append((t, None, None, "MISSING"))
        continue
    r = spark.sql(f"SELECT count(*) n, max(_ingest_ts) last FROM {CATALOG}.bronze.{t}").first()
    rows.append((t, r.n, str(r.last), "ok" if r.n > 0 else "EMPTY"))
result = spark.createDataFrame(rows, "table string, row_count long, last_ingest string, status string")
display(result)

# COMMAND ----------

# Schema drift parked, not lost (Auto Loader rescue / COPY INTO)
for t in ["wiki_current_programming"]:
    if t in existing and "_rescued_data" in spark.table(f"{CATALOG}.bronze.{t}").columns:
        display(spark.sql(f"SELECT count(*) AS rescued_rows FROM {CATALOG}.bronze.{t} WHERE _rescued_data IS NOT NULL"))

# COMMAND ----------

# Load history: one entry per COPY INTO / streaming batch
for t in [t for t in tables if t in existing]:
    print(t)
    display(spark.sql(f"DESCRIBE HISTORY {CATALOG}.bronze.{t}").select("version", "timestamp", "operation").limit(5))

# COMMAND ----------

# gdrive_mridul is legitimately absent/empty when Mridul.xlsx wasn't in Temp (notebook 04 skips)
bad = result.filter("status != 'ok' AND table != 'gdrive_mridul'").count()
if bad:
    raise Exception(f"{bad} bronze table(s) missing or empty - see the first cell")   # fails the Job task