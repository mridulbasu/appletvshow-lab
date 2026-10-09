# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 – Manual load: `Apple Tv Show.xlsx` → `appletvshow.bronze.manual_apple_tv_show`
# MAGIC **Covers 2.1 (local files, batch) · 2.2 (COPY INTO) · 2.6 (why COPY INTO here)**
# MAGIC
# MAGIC **Upload first (UI)** **[2.1 local files]**
# MAGIC 1. In Databricks: **New → Add or upload data → Upload files to a volume**.
# MAGIC 2. Pick volume `appletvshow.bronze.landing`, folder `manual_load`.
# MAGIC 3. Drag in `Apple Tv Show.xlsx` from your Desktop\Dashboard folder.
# MAGIC
# MAGIC The UI accepts files up to 5 GB. You need `WRITE VOLUME`, `USE SCHEMA` and `USE CATALOG`
# MAGIC ([upload files](https://docs.databricks.com/aws/en/ingestion/file-upload/)).
# MAGIC
# MAGIC **Why COPY INTO and not Auto Loader** **[2.6]**: this is a handful of manual drops, which is COPY INTO's sweet spot.
# MAGIC Auto Loader's Excel support has no schema evolution ([Excel](https://docs.databricks.com/aws/en/query/formats/excel)).

# COMMAND ----------

import re

#CATALOG = "appletvshow"
dbutils.widgets.text("catalog", "appletvshow")
CATALOG = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {CATALOG}")

SRC = f"/Volumes/{CATALOG}/bronze/landing/manual_load/"
TARGET = f"{CATALOG}.bronze.manual_apple_tv_show"

display(dbutils.fs.ls(SRC))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect the workbook  **[Excel format options: `operation = listSheets`, `headerRows`, `dataAddress`]**
# MAGIC Native Excel reading needs DBR 17.1+ and supports `.xls` and `.xlsx`.
# MAGIC Merged cells keep only the top-left value, and only one header row is supported
# MAGIC ([Excel](https://docs.databricks.com/aws/en/query/formats/excel)).

# COMMAND ----------

xlsx_files = [f.path for f in dbutils.fs.ls(SRC) if f.name.lower().endswith(".xlsx")]
assert xlsx_files, f"No .xlsx in {SRC} - upload the workbook first"
print(xlsx_files)
display(spark.read.format("excel").option("operation", "listSheets").load(xlsx_files[0]))

# COMMAND ----------

# Set this if the data isn't on the first sheet, e.g. "Shows!A1" ; None = first sheet
DATA_ADDRESS = None

opts = "format => 'excel', headerRows => 1" + (f", dataAddress => '{DATA_ADDRESS}'" if DATA_ADDRESS else "")
preview = spark.sql(f"SELECT * FROM read_files('{SRC}', {opts})")
display(preview.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### COPY INTO with audit columns  **[2.2 COPY INTO → UC-governed table]**
# MAGIC - The source is a `SELECT` over the files, so audit columns can be added during the load
# MAGIC   ([COPY INTO reference](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into)).
# MAGIC - Excel headers often contain spaces, so the select list renames them to snake_case.
# MAGIC - `_metadata.file_path` records which file each row came from
# MAGIC   ([file metadata](https://docs.databricks.com/aws/en/ingestion/file-metadata-column)).

# COMMAND ----------

def snake(c):
    return re.sub(r"[^0-9a-zA-Z]+", "_", c).strip("_").lower()

data_cols = [c for c in preview.columns if c != "_rescued_data"]   # read_files may add this; COPY INTO handles its own
select_list = ",\n         ".join(f"`{c}` AS {snake(c)}" for c in data_cols)
fmt_opts = "'headerRows' = '1'" + (f", 'dataAddress' = '{DATA_ADDRESS}'" if DATA_ADDRESS else "")

# One-time table setup. Kept out of the load cell: every ALTER TABLE creates a new version with no new rows.
spark.sql(f"CREATE TABLE IF NOT EXISTS {TARGET}")   # schemaless; COPY INTO sets the schema
props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {TARGET}").collect()}
if props.get("delta.feature.timestampNtz") != "supported":     # Excel dates can infer as TIMESTAMP_NTZ
    spark.sql(f"ALTER TABLE {TARGET} SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')")

# COMMAND ----------

# FORCE = False -> normal, idempotent: files already loaded are skipped.
# FORCE = True  -> 'force' = 'true': reloads EVERY matching file, even ones already loaded -> duplicate rows.
FORCE = False

copy_opts = "'mergeSchema' = 'true'" + (", 'force' = 'true'" if FORCE else "")

copy_sql = f"""
COPY INTO {TARGET}
FROM (
  SELECT {select_list},
         'manual_load'        AS _source_system,
         current_timestamp()  AS _ingest_ts,
         _metadata.file_path  AS _source_file
  FROM '{SRC}'
)
FILEFORMAT = EXCEL
PATTERN = '*.xlsx'
FORMAT_OPTIONS ({fmt_opts})
COPY_OPTIONS ({copy_opts})
"""
print(copy_sql)
display(spark.sql(copy_sql))   # shows num_affected_rows, num_inserted_rows

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prove idempotency  **[2.2: already-loaded files are skipped]**
# MAGIC 1. Rerun the cell above: expect **0** inserted rows.
# MAGIC 2. Upload a second workbook to `manual_load/`, rerun: only the new file loads.
# MAGIC 3. Set `FORCE = True` and rerun: every file reloads, even already-loaded ones. Row count jumps, giving duplicates.
# MAGIC    This is `'force' = 'true'` in `COPY_OPTIONS`, which turns idempotency off. Set it back to `False` afterwards.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TARGET}").select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Version compare: which rows did the latest load add?  **[Delta time travel + EXCEPT]**
# MAGIC Compares the newest **COPY INTO** version with the version just before it, ignoring the audit columns.
# MAGIC Versions from `SET TBLPROPERTIES` or a COPY INTO that skipped every file add no rows.
# MAGIC Pick versions where `operationMetrics` shows rows written. Set `NEW_V` / `OLD_V` by hand to compare any pair.
# MAGIC Syntax: [time travel](https://docs.databricks.com/aws/en/delta/history).

# COMMAND ----------

hist = spark.sql(f"DESCRIBE HISTORY {TARGET}").select("version", "operation", "operationMetrics")
display(hist)

copy_versions = [r.version for r in hist.filter("operation LIKE '%COPY%'").orderBy("version", ascending=False).collect()]
NEW_V = copy_versions[0]          # latest COPY INTO (override by hand if needed)
OLD_V = NEW_V - 1                 # the version just before it

AUDIT = "_source_system, _ingest_ts, _source_file"
diff_sql = f"""
SELECT * EXCEPT ({AUDIT}) FROM {TARGET} VERSION AS OF {NEW_V}
EXCEPT
SELECT * EXCEPT ({AUDIT}) FROM {TARGET} VERSION AS OF {OLD_V}
"""
print(diff_sql)
display(spark.sql(diff_sql))      # rows present in NEW_V that weren't in OLD_V