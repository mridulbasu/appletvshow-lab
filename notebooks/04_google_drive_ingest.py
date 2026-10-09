# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 – Google Drive: `My Drive / Temp / Mridul.xlsx` → `appletvshow.bronze.gdrive_mridul`
# MAGIC **Covers 2.1 (standard connector) · 2.4 (Lakeflow Connect connection) · 2.2 (COPY INTO) · 2.6 (method choice) · 2.7 (file bytes via `binaryFile`)**
# MAGIC
# MAGIC Flow: **check** that `Mridul.xlsx` exists in Temp → if not, **skip** cleanly → if yes, **land** a dated copy in the
# MAGIC `landing/sharepoint/` folder → **COPY INTO** bronze.
# MAGIC
# MAGIC **[2.6] Why the standard connector:** you want one named file, picked conditionally.
# MAGIC The managed Google Drive pipeline ingests whole folders and is set up through the API only (no UI)
# MAGIC ([Ingest files from Google Drive](https://docs.databricks.com/gcp/en/ingestion/google-drive)).
# MAGIC The standard connector needs DBR 17.3+. **TBD:** confirm your serverless environment meets this.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisite – UC connection  **[2.4: connection = UC securable that stores credentials]**
# MAGIC 1. In Databricks: **Catalog → Create → Create a connection**, type **Google Drive**.
# MAGIC 2. Auth: **OAuth U2M: Databricks-managed** (no Google Cloud project needed). Then **Sign in with Google**.
# MAGIC 3. Name it `gdrive_personal`.
# MAGIC
# MAGIC Don't share it: sharing a personal-account connection gives others access to your Drive
# MAGIC ([Google Drive connection](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/google-drive-connection)).
# MAGIC
# MAGIC **Temp folder URL:** in Google Drive, open My Drive → Temp, then copy the browser address.
# MAGIC It looks like `https://drive.google.com/drive/folders/<long-id>`. Paste it below.

# COMMAND ----------

import datetime
import os
import re

CATALOG = "appletvshow"
CONN = "googledrive"
TEMP_FOLDER_URL = "https://drive.google.com/drive/folders/0B0ehKxnPur13QmpjbnlqTDVLMlU"   # <-- paste your Temp folder URL
FILE_NAME = "Mridul.xlsx"                                                   # exact name, case-sensitive
LANDING_GD = f"/Volumes/{CATALOG}/bronze/landing/sharepoint"   # reuses the folder made by 00_setup_and_map
TARGET = f"{CATALOG}.bronze.gdrive_mridul"

assert "<TEMP_FOLDER_ID>" not in TEMP_FOLDER_URL, "Paste your Temp folder URL first"
assert os.path.isdir(LANDING_GD), 'Run 00_setup_and_map first (it creates landing/sharepoint)'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 – Does `Mridul.xlsx` exist?  **[2.1 standard connector · 2.7 `binaryFile` = one row per file]**
# MAGIC - `pathGlobFilter` keeps only files with that exact name.
# MAGIC   It works for uploaded files like `.xlsx`, but not for native Google formats such as a Google Sheet with no extension.
# MAGIC - Batch reads always search subfolders (`recursiveFileLookup = false` isn't supported).
# MAGIC   A `Mridul.xlsx` inside a subfolder of Temp would also match.
# MAGIC - Google Drive allows two files with the same name, so if several match, the newest is used.

# COMMAND ----------

matches = (spark.read.format("binaryFile")
           .option("databricks.connection", CONN)
           .option("pathGlobFilter", FILE_NAME)
           .load(TEMP_FOLDER_URL)
           .select("path", "modificationTime", "length", "content")
           .orderBy("modificationTime", ascending=False)
           .collect())

found = len(matches) > 0
try:
    dbutils.jobs.taskValues.set(key="gdrive_file_found", value=found)   # lets a Job If/else task branch on it (Section 4)
except Exception:
    pass                                                                # not running as a Job task

if not found:
    dbutils.notebook.exit(f"SKIPPED: {FILE_NAME} not found in Temp - nothing ingested")

if len(matches) > 1:
    print(f"WARNING: {len(matches)} files named {FILE_NAME}; using the newest")
latest = matches[0]
print(f"Found {latest['path']}  ({latest['length']:,} bytes, modified {latest['modificationTime']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 – Land a dated copy in `landing/sharepoint/<date>/`  **[bronze keeps the source as received]**
# MAGIC The volume copy gives you history (a snapshot per day) and lineage (Drive → volume → table).
# MAGIC COPY INTO then reads from the volume, with no connection needed.

# COMMAND ----------

run_date = datetime.date.today().isoformat()
dest_dir = f"{LANDING_GD}/{run_date}"
os.makedirs(dest_dir, exist_ok=True)
with open(f"{dest_dir}/{FILE_NAME}", "wb") as f:
    f.write(latest["content"])
print(f"Landed -> {dest_dir}/{FILE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 – COPY INTO bronze  **[2.2 COPY INTO, idempotent]**
# MAGIC - Each day's snapshot is a new path, so it loads. A same-day rerun is skipped as already loaded.
# MAGIC - `_snapshot_date` (taken from the folder name) tells you which day's version each row is from.

# COMMAND ----------

def snake(c):
    return re.sub(r"[^0-9a-zA-Z]+", "_", c).strip("_").lower()

preview = spark.sql(f"SELECT * FROM read_files('{dest_dir}/{FILE_NAME}', format => 'excel', headerRows => 1)")
display(preview.limit(10))

cols = [c for c in preview.columns if c != "_rescued_data"]
select_list = ",\n         ".join(f"`{c}` AS {snake(c)}" for c in cols)

# one-time table setup (kept out of the load so reruns don't add empty versions)
spark.sql(f"CREATE TABLE IF NOT EXISTS {TARGET}")
props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {TARGET}").collect()}
if props.get("delta.feature.timestampNtz") != "supported":      # Excel dates can infer as TIMESTAMP_NTZ
    spark.sql(f"ALTER TABLE {TARGET} SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')")

copy_sql = f"""
COPY INTO {TARGET}
FROM (
  SELECT {select_list},
         'google_drive'        AS _source_system,
         current_timestamp()   AS _ingest_ts,
         _metadata.file_path   AS _source_file,
         to_date(regexp_extract(_metadata.file_path, '(\\\\d{{4}}-\\\\d{{2}}-\\\\d{{2}})', 1)) AS _snapshot_date
  FROM '{LANDING_GD}/'
)
FILEFORMAT = EXCEL
PATTERN = '*/{FILE_NAME}'
FORMAT_OPTIONS ('headerRows' = '1')
COPY_OPTIONS ('mergeSchema' = 'true')
"""
print(copy_sql)
display(spark.sql(copy_sql))     # num_inserted_rows = 0 means today's snapshot was already loaded

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional – read straight from Drive, no landing copy  **[2.1 / 2.3 alternatives]**
# MAGIC ```python
# MAGIC # Batch, one file. Use the file's own URL; native Google Sheets are exported as XLSX automatically
# MAGIC df = (spark.read.format("excel").option("databricks.connection", CONN)
# MAGIC         .option("headerRows", 1).load(latest["path"]))
# MAGIC
# MAGIC # Incremental: Auto Loader on the whole Temp folder (note: Auto Loader + Excel has no schema evolution)
# MAGIC (spark.readStream.format("cloudFiles")
# MAGIC    .option("cloudFiles.format", "excel")
# MAGIC    .option("databricks.connection", CONN)
# MAGIC    .option("pathGlobFilter", FILE_NAME)
# MAGIC    .option("cloudFiles.schemaLocation", f"/Volumes/{CATALOG}/bronze/checkpoints/google_drive/schema")
# MAGIC    .load(TEMP_FOLDER_URL)
# MAGIC  .writeStream.option("checkpointLocation", f"/Volumes/{CATALOG}/bronze/checkpoints/google_drive/cp")
# MAGIC    .trigger(availableNow=True).toTable(f"{CATALOG}.bronze.gdrive_stream"))
# MAGIC ```