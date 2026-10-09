# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 11 – Bronze → Silver: clean the Excel tables  **[3.1 cleaning · 3.4 dedup]**
# MAGIC The same cleaning ideas, written **generically** because the Excel columns are yours, not Wikipedia's:
# MAGIC - trim text, and turn blanks into NULL
# MAGIC - drop rows that are completely empty (blank Excel rows)
# MAGIC - remove the duplicates that COPY INTO reloads created (`force`, re-uploads)
# MAGIC - Google Drive: keep only the **latest daily snapshot**
# MAGIC - add a `title_key` so notebook 12 can join to Wikipedia
# MAGIC
# MAGIC | Bronze | Silver |
# MAGIC | --- | --- |
# MAGIC | `bronze.manual_apple_tv_show` | `silver.manual_shows` |
# MAGIC | `bronze.gdrive_mridul` (if it exists) | `silver.gdrive_mridul` |

# COMMAND ----------

import re
from pyspark.sql import functions as F, Window

CATALOG = "appletvshow"
SOURCES = {"manual_apple_tv_show": "manual_shows", "gdrive_mridul": "gdrive_mridul"}
AUDIT = {"_source_system", "_ingest_ts", "_source_file", "_snapshot_date", "_rescued_data"}
# Set this if auto-detection picks the wrong column, e.g. "show_name"; None = auto
MANUAL_TITLE_COL = None

existing = {r.tableName for r in spark.sql(f"SHOW TABLES IN {CATALOG}.bronze").collect()}

# COMMAND ----------

def clean_excel(df):
    data_cols = [c for c in df.columns if c not in AUDIT]
    # 1. trim every string column; '' -> NULL                                     [3.1 nulls]
    for c, t in df.dtypes:
        if c in data_cols and t == "string":
            df = df.withColumn(c, F.nullif(F.trim(F.col(c)), F.lit("")))
    # 2. drop rows where every data column is NULL (blank spreadsheet rows)         [3.1]
    df = df.dropna(how="all", subset=data_cols)
    # 3. Google Drive snapshots: keep only the latest day                            [3.4]
    if "_snapshot_date" in df.columns:
        latest_day = df.agg(F.max("_snapshot_date")).first()[0]
        df = df.filter(F.col("_snapshot_date") == F.lit(latest_day))
    # 4. duplicates from reloads: same data values -> keep the most recent load      [3.4]
    w = Window.partitionBy(*data_cols).orderBy(F.col("_ingest_ts").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return df, data_cols


def find_title_col(cols):
    if MANUAL_TITLE_COL:
        return MANUAL_TITLE_COL
    hits = [c for c in cols if re.search(r"title|show|series|program|name", c)]
    return hits[0] if hits else None

# COMMAND ----------

for bronze_name, silver_name in SOURCES.items():
    if bronze_name not in existing:
        print(f"skip {bronze_name}: not in bronze")
        continue
    raw = spark.read.table(f"{CATALOG}.bronze.{bronze_name}")
    clean, data_cols = clean_excel(raw)

    title_col = find_title_col(data_cols)
    if title_col:
        clean = clean.withColumn("title_key", F.lower(F.regexp_replace(F.col(title_col).cast("string"), r"[^A-Za-z0-9]", "")))
    print(f"{bronze_name}: {raw.count():,} bronze rows -> {clean.count():,} silver rows; title column = {title_col}")

    (clean.drop("_rescued_data")
          .write.mode("overwrite").option("overwriteSchema", "true")
          .saveAsTable(f"{CATALOG}.silver.{silver_name}"))
    display(spark.table(f"{CATALOG}.silver.{silver_name}").limit(10))