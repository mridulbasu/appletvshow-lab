# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 12 – Combining DataFrames  **[3.2 inner, left, broadcast, multiple keys, cross join, union, union all]**
# MAGIC Wikipedia's current shows (`silver.wiki_shows`) combined with your own Excel list (`silver.manual_shows`).
# MAGIC If notebook 11 couldn't find a title column in your Excel file, a small built-in list stands in, so every cell still runs.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "appletvshow"
shows = spark.read.table(f"{CATALOG}.silver.wiki_shows")

existing = {r.tableName for r in spark.sql(f"SHOW TABLES IN {CATALOG}.silver").collect()}
manual = None
if "manual_shows" in existing:
    m = spark.read.table(f"{CATALOG}.silver.manual_shows")
    if "title_key" in m.columns:
        manual = m
if manual is None:
    print("Using a built-in sample list (no title_key in silver.manual_shows)")
    manual = (spark.createDataFrame([("Severance",), ("Ted Lasso",), ("Slow Horses",), ("A Show Not On Apple TV",)], "title string")
              .withColumn("title_key", F.lower(F.regexp_replace("title", r"[^A-Za-z0-9]", ""))))

manual_keys = manual.select("title_key").dropna().distinct()
print(f"wiki shows: {shows.count():,} | your list: {manual_keys.count():,} distinct titles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Inner join: only matches on both sides  **[3.2 inner]**
# MAGIC Joining **on a column name** (`"title_key"`) returns that key once. Joining on an expression
# MAGIC (`a.k == b.k`) keeps both copies, a classic "ambiguous column" trap.

# COMMAND ----------

inner = shows.join(manual_keys, on="title_key", how="inner")
print(f"inner: {inner.count():,} of your titles are on the current Wikipedia list")
display(inner.select("title", "category", "genre", "status_group"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Left join, and anti / semi  **[3.2 left]**
# MAGIC - **left:** every Wikipedia show, plus a flag showing whether it's on your list (NULL when not).
# MAGIC - **left_anti:** rows on the left with **no** match. Here: titles in your Excel file that aren't currently airing.
# MAGIC - **left_semi:** rows on the left **with** a match, keeping left columns only.

# COMMAND ----------

flagged = (shows.join(manual_keys.withColumn("on_my_list", F.lit(True)), "title_key", "left")
                .withColumn("on_my_list", F.coalesce("on_my_list", F.lit(False))))
display(flagged.groupBy("on_my_list").count())

not_airing = manual.join(shows.select("title_key"), "title_key", "left_anti")
print(f"left_anti: {not_airing.count():,} of your titles are not in Wikipedia's current programming")
display(not_airing)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Cross join + multiple-key join: a complete category × year grid  **[3.2 cross join, multiple keys]**
# MAGIC 1. **Cross join** gives every category paired with every year. Rows = categories × years. Use it deliberately.
# MAGIC 2. **Left join on two keys** (`category`, `premiere_year`) fills in counts. Gaps become 0 instead of disappearing.

# COMMAND ----------

categories = shows.select("category").distinct()
years = spark.range(2019, 2027).withColumnRenamed("id", "premiere_year")       # Apple TV+ launched in 2019
grid = categories.crossJoin(years)
print(f"cross join: {categories.count()} categories x {years.count()} years = {grid.count()} rows")

premieres = shows.groupBy("category", "premiere_year").agg(F.count("*").alias("premieres"))
by_year = (grid.join(premieres, on=["category", "premiere_year"], how="left")        # multiple keys
               .fillna({"premieres": 0})
               .orderBy("category", "premiere_year"))
display(by_year)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Broadcast join: small lookup table  **[3.2 broadcast · links to 3.5]**
# MAGIC `F.broadcast(small)` (or SQL `/*+ BROADCAST(t) */`) ships the small table to every executor, so the big side isn't shuffled.
# MAGIC The hint applies regardless of `autoBroadcastJoinThreshold`
# MAGIC ([join hints](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-hints)).
# MAGIC Look for **BroadcastHashJoin** in the plan.

# COMMAND ----------

lookup = spark.createDataFrame(
    [("Drama", "Scripted"), ("Comedy", "Scripted"), ("International", "Scripted"),
     ("Documentaries", "Unscripted"), ("Sports", "Unscripted")],
    "category string, category_group string")

grouped = shows.join(F.broadcast(lookup), "category", "left")
grouped.explain()                       # find BroadcastHashJoin in the physical plan
display(grouped.groupBy("category_group").count())

# COMMAND ----------

shows.createOrReplaceTempView("v_shows")
lookup.createOrReplaceTempView("v_lookup")
display(spark.sql("""
  SELECT /*+ BROADCAST(l) */ l.category_group, count(*) AS shows
  FROM v_shows s LEFT JOIN v_lookup l ON s.category = l.category
  GROUP BY ALL"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Union vs union all: the classic trap  **[3.2 union, union all]**
# MAGIC | API | Duplicates | Columns matched by |
# MAGIC | --- | --- | --- |
# MAGIC | DataFrame `.union()` / `.unionAll()` | **kept** | **position** ([union](https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/union)) |
# MAGIC | DataFrame `.unionByName()` | kept | **name** (`allowMissingColumns=True` fills gaps with NULL) |
# MAGIC | SQL `UNION` | **removed** (DISTINCT is the default) | position |
# MAGIC | SQL `UNION ALL` | kept | position ([set operators](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-setops)) |

# COMMAND ----------

wiki_titles = shows.select(F.col("title_key"), F.lit("wiki").alias("source"))
my_titles = manual_keys.select(F.lit("my_list").alias("source"), F.col("title_key"))   # columns in the OTHER order

wrong = wiki_titles.union(my_titles)          # matched by position -> 'my_list' lands in title_key!
right = wiki_titles.unionByName(my_titles)    # matched by name
display(wrong.filter("title_key = 'my_list'").limit(3))
print(f"union rows: {right.count():,} | distinct title_keys: {right.select('title_key').distinct().count():,}")

wiki_titles.select("title_key").createOrReplaceTempView("t_wiki")
my_titles.select("title_key").createOrReplaceTempView("t_mine")
display(spark.sql("""
  SELECT 'UNION'     AS op, count(*) AS row_count FROM (SELECT title_key FROM t_wiki UNION     SELECT title_key FROM t_mine)
  UNION ALL
  SELECT 'UNION ALL' AS op, count(*) AS row_count FROM (SELECT title_key FROM t_wiki UNION ALL SELECT title_key FROM t_mine)"""))