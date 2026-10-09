# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 14 – Tuning parameters, then re-measure  **[3.5]**
# MAGIC The Apple TV data has a few hundred rows, far too small to show any difference. So this notebook generates
# MAGIC **5 million synthetic "viewing events"** spread across the real shows, then times one join plus aggregation
# MAGIC under different settings.
# MAGIC
# MAGIC | Parameter | Controls | Serverless / Free Edition |
# MAGIC | --- | --- | --- |
# MAGIC | `spark.sql.shuffle.partitions` | partitions after a shuffle (joins, groupBy) | **settable**; default `auto` |
# MAGIC | `spark.sql.autoBroadcastJoinThreshold` | max table size broadcast automatically | not in serverless' allowed list; use `F.broadcast` / hints. Classic: settable |
# MAGIC | `spark.default.parallelism` | default partitions for **RDD** operations | not available (no RDDs on serverless). Docs-only |
# MAGIC | `spark.executor.memory` / `spark.driver.memory` | memory per executor / driver | set by node types on classic compute. Docs-only |
# MAGIC
# MAGIC Serverless allows only a short list of Spark confs ([Spark conf on serverless](https://docs.databricks.com/aws/en/spark/conf)).
# MAGIC AQE is on by default: it coalesces partitions, switches to broadcast joins at runtime (30 MB threshold) and splits
# MAGIC skewed partitions ([AQE](https://docs.databricks.com/aws/en/optimizations/aqe)).

# COMMAND ----------

import time
from pyspark.sql import functions as F, Window

#CATALOG = "appletvshow"
dbutils.widgets.text("catalog", "appletvshow")
CATALOG = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {CATALOG}")

N_EVENTS = 5_000_000

for k in ["spark.sql.shuffle.partitions", "spark.sql.autoBroadcastJoinThreshold",
          "spark.default.parallelism", "spark.executor.memory", "spark.driver.memory"]:
    try:
        print(f"{k:45} = {spark.conf.get(k)}")
    except Exception as e:
        print(f"{k:45} -> not readable here ({type(e).__name__})")

# COMMAND ----------

shows = (spark.read.table(f"{CATALOG}.silver.wiki_shows")
         .select("title", "category")
         .withColumn("show_idx", F.row_number().over(Window.orderBy("title")) - 1))
n_shows = shows.count()

events = (spark.range(N_EVENTS)
          .select(F.col("id").alias("event_id"),
                  (F.rand(42) * n_shows).cast("int").alias("show_idx"),
                  (F.rand(7) * 60).cast("int").alias("minutes_watched")))

def workload(ev, sh):
    return (ev.join(sh, "show_idx")
              .groupBy("category", "title")
              .agg(F.count("*").alias("views"), F.sum("minutes_watched").alias("minutes")))

results = []
def measure(label, df):
    t0 = time.perf_counter()
    rows = len(df.collect())                       # an action forces the work to run
    secs = round(time.perf_counter() - t0, 2)
    results.append((label, secs, rows))
    print(f"{label:45} {secs:6} s  ({rows} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experiment 1 – `spark.sql.shuffle.partitions`
# MAGIC Too few partitions gives big tasks that may spill to disk. Too many gives thousands of tiny tasks, all overhead.
# MAGIC Run each one, then open the **query profile** (link under the cell) and compare task counts.

# COMMAND ----------

original = spark.conf.get("spark.sql.shuffle.partitions")
measure("warm-up (ignore)", workload(events, shows))
for setting in ["auto", "8", "400"]:
    spark.conf.set("spark.sql.shuffle.partitions", setting)
    measure(f"shuffle.partitions = {setting}", workload(events, shows))
spark.conf.set("spark.sql.shuffle.partitions", original)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experiment 2 – broadcast vs shuffle join
# MAGIC `shows` is tiny, so AQE will usually broadcast it on its own. The `merge` hint forces a sort-merge join for comparison
# MAGIC ([join hints](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-hints)).
# MAGIC In the plans, look for **BroadcastHashJoin** vs **SortMergeJoin**.

# COMMAND ----------

measure("join: forced sort-merge (hint 'merge')", workload(events, shows.hint("merge")))
measure("join: forced broadcast (F.broadcast)", workload(events, F.broadcast(shows)))
workload(events, shows.hint("merge")).explain()
workload(events, F.broadcast(shows)).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experiment 3 – `autoBroadcastJoinThreshold`
# MAGIC On classic compute, `-1` turns automatic broadcasting off, and raising it broadcasts bigger tables.
# MAGIC On serverless, expect this to be refused. That refusal is itself the lesson: on serverless you steer joins with
# MAGIC **hints** rather than this setting. **TBD:** the static default and the `-1` behaviour come from the Apache Spark docs,
# MAGIC not docs.databricks.com.

# COMMAND ----------

try:
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    measure("autoBroadcastJoinThreshold = -1", workload(events, shows))
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
except Exception as e:
    print("Not settable here:", str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Re-measure: the results side by side
# MAGIC Change **one** setting at a time, keep the change only if it helps, and remember AQE may already be doing the job.
# MAGIC Timings vary run to run, so repeat a test before trusting a small difference.

# COMMAND ----------

display(spark.createDataFrame(results, "experiment string, seconds double, result_rows long"))