# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 13 – Reshape, deduplicate, aggregate  **[3.3 columns/rows/explode · 3.4 dedup + aggregates]**

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOG = "appletvshow"
shows = spark.read.table(f"{CATALOG}.silver.wiki_shows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Columns and rows  **[3.3 add, drop, split, rename, filter]**

# COMMAND ----------

reshaped = (shows
    .withColumn("is_renewed", F.col("status_group") == "Renewed")                 # add
    .withColumn("runtime_band", F.when(F.col("runtime_max") <= 35, "short")       # add with logic
                                 .when(F.col("runtime_max") <= 60, "standard")
                                 .otherwise("long"))
    .withColumn("title_words", F.split("title", " "))                              # split -> ARRAY
    .withColumn("first_word", F.col("title_words")[0])                             # take one element
    .withColumnRenamed("category", "wiki_section")                                 # rename
    .drop("seasons_raw", "runtime_raw", "premiere_raw")                            # drop
    .filter(F.col("premiere_year") >= 2022)                                        # filter (= where)
)
display(reshaped.select("title", "wiki_section", "is_renewed", "runtime_band", "title_words", "first_word").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explode arrays  **[3.3 exploding arrays]**
# MAGIC Notebook 10 split `genre` into the array `genres`. `explode` turns **one row per show** into **one row per show-genre**.
# MAGIC | Function | Empty or NULL array | Extra column |
# MAGIC | --- | --- | --- |
# MAGIC | `explode` | row **dropped** | – |
# MAGIC | `explode_outer` | row **kept** with NULL | – |
# MAGIC | `posexplode` | dropped | position (0 = first genre) |

# COMMAND ----------

exploded = shows.select("title", "category", F.explode("genres").alias("genre_item"))
outer = shows.select("title", F.explode_outer("genres").alias("genre_item"))
pos = shows.select("title", F.posexplode("genres").alias("genre_pos", "genre_item"))
print(f"shows {shows.count():,} | explode {exploded.count():,} | explode_outer {outer.count():,}")
display(pos.filter("genre_pos > 0").limit(10))                  # shows with more than one genre

genres = exploded.withColumn("genre_item", F.initcap(F.trim("genre_item")))
genres.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.wiki_show_genres")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explode semi-structured data: the API's section list (VARIANT)
# MAGIC The same idea in SQL on the VARIANT column from Section 2: `variant_explode` turns the `sections` array into rows
# MAGIC ([variant_explode](https://docs.databricks.com/aws/en/sql/language-manual/functions/variant_explode)).

# COMMAND ----------

display(spark.sql(f"""
  SELECT payload:revid::bigint AS revid, s.pos, s.value:line::string AS section, s.value:level::int AS level
  FROM {CATALOG}.bronze.wiki_api_raw, LATERAL variant_explode(payload:sections) AS s
  ORDER BY revid DESC, s.pos
  LIMIT 30"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Deduplication: four tools, four results  **[3.4]**
# MAGIC Run on **bronze**, where every pulled revision and every reload is still present.
# MAGIC | Tool | Keeps | Use when |
# MAGIC | --- | --- | --- |
# MAGIC | `distinct()` | one copy of rows identical in **every** column | exact duplicates only |
# MAGIC | `dropDuplicates([keys])` | **one arbitrary** row per key | any copy will do |
# MAGIC | `row_number()` over a window | the row **you choose** per key (e.g. latest) | "latest version wins": silver's usual rule |
# MAGIC | `GROUP BY` + aggregates | one summary row per key | you want totals, not rows |

# COMMAND ----------

b = spark.read.table(f"{CATALOG}.bronze.wiki_current_programming").drop("_ingest_ts", "_source_file", "_rescued_data", "extracted_at")
w = Window.partitionBy("category", "title").orderBy(F.col("source_revid").cast("bigint").desc())
display(spark.createDataFrame([
    ("all bronze rows", b.count()),
    ("distinct()", b.distinct().count()),
    ("dropDuplicates(category, title)", b.dropDuplicates(["category", "title"]).count()),
    ("row_number() latest per show", b.withColumn("rn", F.row_number().over(w)).filter("rn = 1").count()),
], "method string, rows long"))

# COMMAND ----------

# Your Excel bronze table: duplicates created by COPY INTO 'force' and re-uploads
if spark.catalog.tableExists(f"{CATALOG}.bronze.manual_apple_tv_show"):
    m = spark.read.table(f"{CATALOG}.bronze.manual_apple_tv_show")
    biz = [c for c in m.columns if not c.startswith("_")]
    print(f"manual bronze rows {m.count():,} | distinct business rows {m.select(biz).distinct().count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Aggregates  **[3.4 count, approx count distinct, mean, summary]**
# MAGIC - `count("*")` counts rows. `count(col)` **skips NULLs**.
# MAGIC - `approx_count_distinct` is faster on big data, with about 5% error by default (`rsd = 0.05`).
# MAGIC   Below `rsd = 0.01`, use `count_distinct` instead
# MAGIC   ([approx_count_distinct](https://docs.databricks.com/aws/en/pyspark/reference/functions/approx_count_distinct)).
# MAGIC - `mean` and `avg` are the same. `summary()` gives count, mean, stddev, min, quartiles and max
# MAGIC   ([summary](https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/summary)).

# COMMAND ----------

display(shows.groupBy("category").agg(
    F.count("*").alias("rows"),
    F.count("premiere_date").alias("with_premiere_date"),        # NULLs not counted
    F.countDistinct("title_key").alias("distinct_titles"),
    F.approx_count_distinct("title_key").alias("approx_titles"),
    F.approx_count_distinct("title_key", rsd=0.01).alias("approx_titles_1pct"),
    F.round(F.mean("season_count"), 2).alias("avg_seasons"),
    F.max("runtime_max").alias("longest_runtime_min"),
).orderBy(F.desc("rows")))

display(shows.select("season_count", "episode_count", "runtime_min", "runtime_max").summary())