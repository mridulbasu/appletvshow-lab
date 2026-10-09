# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00 – AppleTVShow lab: setup + Section 2 map
# MAGIC Three sources → `appletvshow.bronze`, each through the Section 2 method that fits it.
# MAGIC
# MAGIC | Exam objective (Section 2) | Manual load – `Apple Tv Show.xlsx` (laptop) | Wikipedia – Apple TV current programming | SharePoint – `UserList.xlsx` (NHPRI site) |
# MAGIC | --- | --- | --- | --- |
# MAGIC | **2.1** Patterns: batch / streaming / incremental; local files, standard and managed connectors | Local file upload to a volume; **batch** | **Incremental** batch (Auto Loader `availableNow`) | **Standard connector** (runs) + **managed connector** (reference spec) |
# MAGIC | **2.2** COPY INTO → UC tables | **Yes** – `FILEFORMAT = EXCEL`, idempotent | – | **Yes** – on the landed daily snapshot |
# MAGIC | **2.3** Auto Loader, schema enforcement/evolution, directory listing vs notification | Not used: Auto Loader for Excel has no schema evolution | **Yes** – JSON, `addNewColumns`, rescued data, directory listing | Optional cell: Auto Loader straight on the SharePoint URL |
# MAGIC | **2.4** Configure Lakeflow Connect | – | – | **Yes** – UC connection + managed ingestion pipeline spec |
# MAGIC | **2.5** JDBC/ODBC or REST client in a notebook, scheduled by Lakeflow Jobs | – | **Yes** – MediaWiki REST API via `urllib` | – |
# MAGIC | **2.6** Choose the method | Few files, manual drops → COPY INTO | No connector exists for a web page → notebook + Auto Loader | Single file → standard connector; managed can't select one file |
# MAGIC | **2.7** Semi-structured / unstructured | – | **Yes** – raw API JSON as **VARIANT** | **Yes** – raw `.xlsx` bytes via `binaryFile`; managed `BINARYFILE` |
# MAGIC
# MAGIC Run order: `00` → `01` → `02` → `03` → `04` → `05`.
# MAGIC Then schedule `01`–`05` as one Lakeflow Job (see the last cell).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 – Catalog, schemas, and the landing "folders"
# MAGIC **[Governance foundation for every objective: "Unity Catalog–governed tables"]**
# MAGIC - Unity Catalog stores all object names in lowercase, so `AppleTVShow` becomes **`appletvshow`**.
# MAGIC   Queries are case-insensitive ([names](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-names)).
# MAGIC - In a serverless workspace, `CREATE CATALOG` without a location uses default storage
# MAGIC   ([default storage](https://docs.databricks.com/aws/en/storage/default-storage)).
# MAGIC   You need the `CREATE CATALOG` privilege ([create catalogs](https://docs.databricks.com/aws/en/catalogs/create-catalog)).
# MAGIC - A schema can't hold folders. Folders live inside a **volume**, so `bronze` gets a `landing` volume
# MAGIC   with one folder per source, plus a `checkpoints` volume for Auto Loader state.

# COMMAND ----------

CATALOG = "appletvshow"   # stored lowercase whatever case you type

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} COMMENT 'Apple TV lab - Section 2 ingestion practice'")
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing COMMENT 'Raw files as received, one folder per source'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.checkpoints COMMENT 'Auto Loader schema + checkpoint state'")

# COMMAND ----------

import os

LANDING = f"/Volumes/{CATALOG}/bronze/landing"
for area in ["manual_load", "wiki", "sharepoint"]:
    os.makedirs(f"{LANDING}/{area}", exist_ok=True)   # folders INSIDE an existing volume

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
display(dbutils.fs.ls(LANDING))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestration – one Lakeflow Job  **[2.5 "orchestrated and scheduled with Lakeflow Jobs"]**
# MAGIC Jobs & Pipelines → Create job → add notebook tasks:
# MAGIC
# MAGIC | Task | Notebook | Depends on |
# MAGIC | --- | --- | --- |
# MAGIC | `manual_load` | 01_manual_load_excel | – |
# MAGIC | `wiki_pull` (1 retry) | 02_wiki_pull | – |
# MAGIC | `wiki_autoload` | 03_wiki_autoload | `wiki_pull` |
# MAGIC | `sharepoint` | 04_sharepoint_ingest | – |
# MAGIC | `validate` | 05_validate_bronze | all four |
# MAGIC
# MAGIC Trigger: a daily schedule, or a file-arrival trigger on `/Volumes/appletvshow/bronze/landing/manual_load/`
# MAGIC ([file arrival triggers](https://docs.databricks.com/aws/en/jobs/file-arrival-triggers)).
# MAGIC Free Edition allows at most 5 concurrent job tasks; this job peaks at 3.