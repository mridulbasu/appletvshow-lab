# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 15 – Gold layer: view, table, materialized view, streaming table  **[3.6]**
# MAGIC | Object | Stores data? | Stays fresh by | Built here as |
# MAGIC | --- | --- | --- | --- |
# MAGIC | **View** | No, recomputed on every query | always current | `gold.v_current_shows` |
# MAGIC | **Table** | Yes | whatever job rewrites it | `gold.shows_by_category` |
# MAGIC | **Materialized view** | Yes | `REFRESH` / schedule. Incremental when possible (needs row tracking on sources), else full | `gold.mv_genre_summary` |
# MAGIC | **Streaming table** | Yes | each refresh processes only **new** rows from an append-only source | `gold.st_wiki_loads` |
# MAGIC
# MAGIC Materialized views and streaming tables are created from a SQL warehouse or a **serverless notebook**, and their refreshes
# MAGIC run on serverless pipelines (billed as pipeline DBUs)
# MAGIC ([materialized views](https://docs.databricks.com/aws/en/ldp/dbsql/materialized),
# MAGIC [streaming tables](https://docs.databricks.com/aws/en/ldp/dbsql/streaming)).
# MAGIC **TBD:** Free Edition allows one active pipeline per pipeline type. If a create fails, the error message says why.

# COMMAND ----------

CATALOG = "appletvshow"

def run(sql, label):
    print(f"--- {label}")
    try:
        spark.sql(sql)
        print("ok")
    except Exception as e:
        print("FAILED:", str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. View: no storage, always current
# MAGIC Typical use: give BI a clean, stable shape (or hide columns) without copying data.

# COMMAND ----------

run(f"""
CREATE OR REPLACE VIEW {CATALOG}.gold.v_current_shows
COMMENT 'Current Apple TV shows, analyst-friendly columns'
AS SELECT title, category, genre, premiere_date, premiere_year, season_count, episode_count, status_group
   FROM {CATALOG}.silver.wiki_shows
""", "view")
display(spark.table(f"{CATALOG}.gold.v_current_shows").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Table: you control when it's rebuilt (CTAS)

# COMMAND ----------

run(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.shows_by_category
COMMENT 'Show counts and averages per Wikipedia section; rebuilt by the job'
AS SELECT category,
          count(*)                         AS shows,
          count_if(status_group = 'Renewed') AS renewed,
          round(avg(season_count), 2)      AS avg_seasons,
          min(premiere_date)               AS first_premiere,
          max(premiere_date)               AS latest_premiere
   FROM {CATALOG}.silver.wiki_shows
   GROUP BY category
""", "table (CTAS)")
display(spark.table(f"{CATALOG}.gold.shows_by_category"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Materialized view: stored results, refreshed on demand or on a schedule
# MAGIC Uncomment the `SCHEDULE` line to refresh it daily without a job.

# COMMAND ----------

#run(f"""
#CREATE OR REPLACE VIEW {CATALOG}.gold.mv_genre_summary
#  -- SCHEDULE CRON '0 0 7 * * ?' AT TIME ZONE 'America/New_York'
#  COMMENT 'Shows per genre (one show can count under several genres)'
#AS SELECT genre_item AS genre, count(DISTINCT title) AS shows
#   FROM {CATALOG}.silver.wiki_show_genres
#   GROUP BY genre_item
#""", "materialized view")
#run(f"REFRESH MATERIALIZED VIEW {CATALOG}.gold.mv_genre_summary", #"refresh MV")
#display(spark.sql(f"SELECT * FROM {CATALOG}.gold.mv_genre_summary #ORDER BY shows DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Streaming table: incremental, append-only source
# MAGIC The source must be **append-only**. Bronze qualifies, because Auto Loader only ever adds rows;
# MAGIC silver doesn't, because notebook 10 overwrites it. Every refresh reads only the rows added since the last one.
# MAGIC Here it builds a "load log" of each show seen in each Wikipedia revision.

# COMMAND ----------

#run(f"""
#CREATE OR REFRESH STREAMING TABLE {CATALOG}.gold.st_wiki_loads
#COMMENT 'One row per show per pulled Wikipedia revision (incremental)'
#AS SELECT category, title, source_revid, _ingest_ts
#   FROM STREAM {CATALOG}.bronze.wiki_current_programming
#""", "streaming table")
#display(spark.sql(f"""
#  SELECT source_revid, count(*) AS shows, max(_ingest_ts) AS loaded_at
#  FROM {CATALOG}.gold.st_wiki_loads GROUP BY source_revid ORDER BY #source_revid DESC"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prove the difference
# MAGIC 1. Run notebooks 02 → 03 → 10 → 13 again after the Wikipedia page has changed (new revision).
# MAGIC 2. **View:** shows the new data immediately.
# MAGIC 3. **Table:** unchanged until you rerun the CTAS cell.
# MAGIC 4. **MV:** unchanged until `REFRESH` (or its schedule).
# MAGIC 5. **Streaming table:** `REFRESH STREAMING TABLE ...st_wiki_loads` adds **only** the new revision's rows.

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.gold"))