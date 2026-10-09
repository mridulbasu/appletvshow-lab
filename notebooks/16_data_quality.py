# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 16 – Data quality checks for silver and gold  **[3.7]**
# MAGIC Three layers of defence, from strictest to softest:
# MAGIC | Layer | Mechanism | A bad row… |
# MAGIC | --- | --- | --- |
# MAGIC | A. Table constraints | Delta `NOT NULL` + `CHECK` | **fails the whole write** ([constraints](https://docs.databricks.com/aws/en/tables/constraints)) |
# MAGIC | B. Rule-based validation | your rules → valid rows to silver, invalid rows to a **quarantine** table | is set aside with the rule it broke |
# MAGIC | C. Pipeline expectations | `@dp.expect` / `expect_or_drop` / `expect_or_fail` (file 16b) | is warned about, dropped, or stops the update ([expectations](https://docs.databricks.com/aws/en/ldp/expectations)) |
# MAGIC
# MAGIC Run this after notebook 10. Rerunning 10 rewrites `silver.wiki_shows` with `overwriteSchema`, so rerun this notebook
# MAGIC afterwards to make sure the constraints are still in place.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "appletvshow"
SILVER = f"{CATALOG}.silver.wiki_shows"

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Constraints: the table itself refuses bad data
# MAGIC - `NOT NULL` and `CHECK` are **enforced**. Adding one checks the existing rows first.
# MAGIC - Primary-key and foreign-key constraints are **informational only** (not enforced).

# COMMAND ----------

existing_checks = {r.key for r in spark.sql(f"SHOW TBLPROPERTIES {SILVER}").collect()}
spark.sql(f"ALTER TABLE {SILVER} ALTER COLUMN title SET NOT NULL")
if "delta.constraints.seasons_nonneg" not in existing_checks:
    spark.sql(f"ALTER TABLE {SILVER} ADD CONSTRAINT seasons_nonneg CHECK (season_count IS NULL OR season_count >= 0)")
if "delta.constraints.runtime_order" not in existing_checks:
    spark.sql(f"ALTER TABLE {SILVER} ADD CONSTRAINT runtime_order CHECK (runtime_min IS NULL OR runtime_max IS NULL OR runtime_min <= runtime_max)")
display(spark.sql(f"SHOW TBLPROPERTIES {SILVER}").filter("key LIKE 'delta.constraints%'"))

# COMMAND ----------

# Try to break them: both inserts should FAIL, and the table is untouched
for label, sql in [
    ("negative seasons", f"INSERT INTO {SILVER} (title, season_count) VALUES ('Bad Row', -1)"),
    ("NULL title",       f"INSERT INTO {SILVER} (title, season_count) VALUES (NULL, 1)"),
]:
    try:
        spark.sql(sql)
        print(f"{label}: inserted (unexpected!)")
    except Exception as e:
        print(f"{label}: rejected -> {str(e).splitlines()[0][:160]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Rule-based validation + quarantine
# MAGIC Constraints are all-or-nothing. Often you'd rather **keep the good rows and park the bad ones** with the reason.
# MAGIC Each rule is a SQL condition that should be TRUE. A rule that returns NULL counts as a fail.

# COMMAND ----------

RULES = {
    "title_present":       "title IS NOT NULL AND title <> ''",
    "category_present":    "category IS NOT NULL",
    "premiere_parsed":     "premiere_raw IS NULL OR premiere_date IS NOT NULL",
    "premiere_not_future": "premiere_date IS NULL OR premiere_date <= current_date()",
    "seasons_nonneg":      "season_count IS NULL OR season_count >= 0",
    "runtime_order":       "runtime_min IS NULL OR runtime_max IS NULL OR runtime_min <= runtime_max",
}

df = spark.read.table(SILVER)
checks = [F.when(~F.coalesce(F.expr(cond), F.lit(False)), F.lit(name)) for name, cond in RULES.items()]
scored = df.withColumn("failed_rules", F.filter(F.array(*checks), lambda x: x.isNotNull()))

valid = scored.filter(F.size("failed_rules") == 0).drop("failed_rules")
invalid = scored.filter(F.size("failed_rules") > 0).withColumn("quarantined_at", F.current_timestamp())

invalid.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.wiki_shows_quarantine")
valid.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.wiki_shows_valid")
print(f"valid {valid.count():,} | quarantined {invalid.count():,}")

# COMMAND ----------

# Data quality report: failures per rule
display(invalid.select(F.explode("failed_rules").alias("rule")).groupBy("rule").count().orderBy(F.desc("count")))
display(invalid.select("title", "category", "premiere_raw", "season_count", "runtime_raw", "failed_rules").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconciliation: did silver lose or invent rows?
# MAGIC Silver should have exactly one row per distinct (category, title) in bronze, minus rows with no title.

# COMMAND ----------

bronze_keys = (spark.read.table(f"{CATALOG}.bronze.wiki_current_programming")
               .filter("title IS NOT NULL AND trim(title) <> ''")
               .select("category", F.trim(F.regexp_replace("title", r"\s+", " ")).alias("title")).distinct().count())
silver_rows = df.count()
print(f"bronze distinct shows {bronze_keys:,} | silver rows {silver_rows:,} | match: {bronze_keys == silver_rows}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Pipeline expectations: see file `16b_pipeline_expectations.py`
# MAGIC Expectations only run inside a **Lakeflow Spark Declarative Pipeline**. To try them:
# MAGIC 1. Open **Jobs & Pipelines → Create → ETL pipeline**.
# MAGIC 2. Set the default catalog to `appletvshow` and the schema to `silver`.
# MAGIC 3. Add `16b_pipeline_expectations.py` as the pipeline's source file.
# MAGIC 4. Run the pipeline, then open the dataset's **Data quality** tab to see how many rows passed, were dropped or failed.
# MAGIC
# MAGIC **TBD:** exact UI labels may differ slightly. Free Edition allows one active pipeline per type.