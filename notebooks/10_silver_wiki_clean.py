# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 10 – Bronze → Silver: clean the Wikipedia shows  **[3.1 data cleaning · 3.4 dedup]**
# MAGIC Reads `bronze.wiki_current_programming` and writes `silver.wiki_shows`: **one row per show**, typed and standardised.
# MAGIC
# MAGIC ### Section 3 map (notebooks 10–16)
# MAGIC | Objective | Notebook | What you do with the Apple TV data |
# MAGIC | --- | --- | --- |
# MAGIC | **3.1** Clean bronze → silver: nulls, data types | **10**, 11 | Text dates → `DATE`, "2 seasons, 19 episodes" → integers, blanks → NULL |
# MAGIC | **3.2** Joins: inner, left, broadcast, multiple keys, cross; union / union all | 12 | Wikipedia shows ↔ your Excel list; category lookup; category × year grid |
# MAGIC | **3.3** Add / drop / split / rename columns, filter, explode arrays | 13 | Split `genre` into an array and explode it; explode the API's section list |
# MAGIC | **3.4** Dedup + aggregates: count, approx count distinct, mean, summary | **10**, 11, 13 | Latest revision per show; duplicates from COPY INTO reloads; stats per category |
# MAGIC | **3.5** Tuning: shuffle partitions, parallelism, memory, broadcast threshold; re-measure | 14 | Time a 5M-row synthetic join/aggregation at different settings |
# MAGIC | **3.6** Gold: views, materialized views, streaming tables, tables | 15 | One of each, built on silver |
# MAGIC | **3.7** Data quality checks | 16 (+16b pipeline) | CHECK / NOT NULL constraints, quarantine table, pipeline expectations |

# COMMAND ----------

from pyspark.sql import functions as F, Window

#CATALOG = "appletvshow"
dbutils.widgets.text("catalog", "appletvshow")
CATALOG = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {CATALOG}")

BRONZE = f"{CATALOG}.bronze.wiki_current_programming"
SILVER = f"{CATALOG}.silver.wiki_shows"

bronze = spark.read.table(BRONZE)
print(f"bronze rows: {bronze.count():,}")
bronze.printSchema()


def col_or_null(df, name):
    """Wikipedia columns vary by table; use NULL when a column doesn't exist."""
    return F.col(name) if name in df.columns else F.lit(None).cast("string")


def to_int(e):
    """regexp_extract returns '' when nothing matches; '' -> NULL, digits -> INT (safe under ANSI mode)."""
    return F.when(e == "", F.lit(None)).otherwise(e.cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 – Profile nulls  **[3.1 "cleaning nulls" starts with measuring them]**

# COMMAND ----------

profile_cols = [c for c in bronze.columns if c != "_rescued_data"]
display(bronze.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in profile_cols]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 – Keep the latest revision of each show  **[3.4 deduplication]**
# MAGIC Bronze appends every page revision you've pulled, so a show can appear several times.
# MAGIC A **window** numbers each show's rows newest-first, and `row_number() = 1` keeps the newest.
# MAGIC `dropDuplicates` would keep an **arbitrary** row, not necessarily the latest
# MAGIC ([dropDuplicates](https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/dropDuplicates)).

# COMMAND ----------

w = Window.partitionBy("category", "title").orderBy(
    F.col("source_revid").cast("bigint").desc(), F.col("_ingest_ts").desc())

latest = (bronze
          .withColumn("_rn", F.row_number().over(w))
          .filter("_rn = 1")
          .drop("_rn"))
print(f"bronze rows {bronze.count():,} -> latest per show {latest.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 – Clean and standardise types  **[3.1]**
# MAGIC | Raw text (bronze) | Cleaned (silver) | How |
# MAGIC | --- | --- | --- |
# MAGIC | `"  Severance "` | `Severance` + `title_key = severance` | `trim`; key = lower-case letters and digits, for joins |
# MAGIC | `"February 18, 2022"` | `2022-02-18` (DATE) | `try_to_timestamp(..., 'MMMM d, yyyy')`, which gives NULL instead of an error on bad text ([docs](https://docs.databricks.com/aws/en/sql/language-manual/functions/try_to_timestamp)) |
# MAGIC | `"2 seasons, 19 episodes"` | `season_count = 2`, `episode_count = 19` | `regexp_extract` → INT |
# MAGIC | `"40–76 min."` | `runtime_min = 40`, `runtime_max = 76` | first and last number |
# MAGIC | `"Renewed for a third season"` | `status_group = Renewed` | `when / otherwise` |
# MAGIC | `"Thriller / Sci-fi"` | `genres = ["Thriller", "Sci-fi"]` (ARRAY) | `split`, used in notebook 13 |

# COMMAND ----------

status_lc = F.lower(F.coalesce(col_or_null(latest, "status"), F.lit("")))
status_group = (F.when(status_lc == "", F.lit(None))
                .when(status_lc.contains("renewed"), "Renewed")
                .when(status_lc.contains("final season") | status_lc.contains("ending"), "Ending")
                .when(status_lc.contains("pending"), "Pending")
                .when(status_lc.contains("miniseries") | status_lc.contains("limited"), "Limited series")
                .otherwise("Other"))

seasons_txt = F.coalesce(col_or_null(latest, "seasons"), F.lit(""))
runtime_txt = F.coalesce(col_or_null(latest, "runtime"), F.lit(""))
premiere_txt = F.coalesce(col_or_null(latest, "premiere"), F.lit(""))

silver = (latest
    # strings: trim, collapse spaces, blank -> NULL
    .withColumn("title", F.trim(F.regexp_replace("title", r"\s+", " ")))
    .filter(F.col("title").isNotNull() & (F.col("title") != ""))
    .withColumn("title_key", F.lower(F.regexp_replace("title", r"[^A-Za-z0-9]", "")))
    .withColumn("genre", F.nullif(F.trim(col_or_null(latest, "genre")), F.lit("")))
    .withColumn("genres", F.filter(F.split(F.coalesce("genre", F.lit("")), r"\s*[/,;]\s*"), lambda g: g != ""))
    # dates
    .withColumn("premiere_raw", F.nullif(premiere_txt, F.lit("")))
    .withColumn("_premiere_str", F.regexp_extract(premiere_txt, r"([A-Z][a-z]+ \d{1,2}, \d{4})", 1))
    .withColumn("premiere_date", F.expr("to_date(try_to_timestamp(nullif(_premiere_str, ''), 'MMMM d, yyyy'))"))
    .withColumn("premiere_year", F.year("premiere_date"))
    # numbers hidden in text
    .withColumn("seasons_raw", F.nullif(seasons_txt, F.lit("")))
    .withColumn("season_count", F.coalesce(to_int(F.regexp_extract(seasons_txt, r"(\d+)\s*season", 1)),
                                           to_int(F.regexp_extract(seasons_txt, r"^\s*(\d+)\s*$", 1))))
    .withColumn("episode_count", to_int(F.regexp_extract(seasons_txt, r"(\d+)\s*episode", 1)))
    .withColumn("runtime_raw", F.nullif(runtime_txt, F.lit("")))
    .withColumn("runtime_min", to_int(F.regexp_extract(runtime_txt, r"(\d+)", 1)))
    .withColumn("runtime_max", to_int(F.regexp_extract(runtime_txt, r"(\d+)(?!.*\d)", 1)))
    # status
    .withColumn("status", F.nullif(F.trim(col_or_null(latest, "status")), F.lit("")))
    .withColumn("status_group", status_group)
    # lineage, typed
    .withColumn("source_revid", F.col("source_revid").cast("bigint"))
    .withColumn("bronze_ingest_ts", F.col("_ingest_ts"))
    .select("category", "subcategory", "title", "title_key", "genre", "genres",
            "premiere_raw", "premiere_date", "premiere_year",
            "seasons_raw", "season_count", "episode_count",
            "runtime_raw", "runtime_min", "runtime_max",
            "status", "status_group", "source_revid", "bronze_ingest_ts"))

display(silver.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 – Check the parsing, then write silver  **[3.1 "write to new silver tables"]**

# COMMAND ----------

display(silver.select(
    F.count("*").alias("rows"),
    F.count("premiere_date").alias("premiere_parsed"),
    F.sum((F.col("premiere_raw").isNotNull() & F.col("premiere_date").isNull()).cast("int")).alias("premiere_unparsed"),
    F.count("season_count").alias("seasons_parsed"),
    F.count("runtime_min").alias("runtime_parsed")))

# Unparsed premieres are worth eyeballing: they tell you which pattern to add
display(silver.filter("premiere_raw IS NOT NULL AND premiere_date IS NULL").select("title", "premiere_raw"))

(silver.write.mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(SILVER))
print(f"wrote {spark.table(SILVER).count():,} rows -> {SILVER}")