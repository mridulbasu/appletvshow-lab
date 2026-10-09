# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "lxml==5.3.0",
# ]
# ///
# MAGIC %md
# MAGIC # 02 – Wikipedia pull: Apple TV "Current programming" → `landing/wiki/`
# MAGIC **Covers 2.5 (REST client in a notebook) · 2.1 (landing for incremental load) · 2.7 (raw API JSON kept for VARIANT)**
# MAGIC
# MAGIC Spark can't read an `https://` page directly. The documented pattern is to download into a volume first
# MAGIC ([download data from the internet](https://docs.databricks.com/aws/en/files/download-internet-files)).
# MAGIC This notebook writes three things per run into `landing/wiki/`:
# MAGIC
# MAGIC | Folder | What | Loaded by |
# MAGIC | --- | --- | --- |
# MAGIC | `api_json/<date>/` | Raw API response (nested JSON) | 03 → `bronze.wiki_api_raw` as VARIANT **[2.7]** |
# MAGIC | `raw_html/<date>/` | Section HTML, audit copy only | not loaded |
# MAGIC | `records/<date>/` | One JSON line per show | 03 → `bronze.wiki_current_programming` via Auto Loader **[2.3]** |

# COMMAND ----------

# MAGIC %pip install lxml==5.3.0

# COMMAND ----------

# %pip doesn't restart Python on its own; restart so the new package is importable
dbutils.library.restartPython()

# COMMAND ----------

import datetime
import json
import os
import re
import urllib.parse
import urllib.request
from io import StringIO

import pandas as pd
from bs4 import BeautifulSoup

CATALOG = "appletvshow"
WIKI = f"/Volumes/{CATALOG}/bronze/landing/wiki"
assert os.path.isdir(WIKI), "Run 00_setup_and_map first"

PAGE = "List_of_Apple_TV_original_programming"
API = "https://en.wikipedia.org/w/api.php"
# Wikimedia asks API clients for a descriptive User-Agent with a contact address; put yours in.
HEADERS = {"User-Agent": "appletvshow-databricks-lab/1.0 (contact: <your-email>)"}
SECTION_ANCHOR = "Current_programming"
EXCLUDE_SUBSECTIONS = {"In development", "Awaiting release"}   # not yet airing


def wiki_parse(**params):
    """[2.5] REST call from a notebook: MediaWiki parse API."""
    params = {"action": "parse", "page": PAGE, "format": "json", "formatversion": 2, **params}
    req = urllib.request.Request(f"{API}?{urllib.parse.urlencode(params)}", headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.load(resp)
    if "error" in payload:
        raise RuntimeError(payload["error"])
    return payload["parse"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Call the API  **[2.5 REST client in a notebook]**

# COMMAND ----------

meta = wiki_parse(prop="sections|revid")            # nested JSON: list of section objects
current = next(s for s in meta["sections"] if s["anchor"] == SECTION_ANCHOR)
parsed = wiki_parse(prop="text|revid", section=current["index"])
revid = parsed["revid"]
run_date = datetime.date.today().isoformat()
print(f"Section {current['index']}, revision {revid}, run {run_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Land raw outputs first  **[bronze keeps the source as received · 2.7 semi-structured]**

# COMMAND ----------

dirs = {k: f"{WIKI}/{k}/{run_date}" for k in ["api_json", "raw_html", "records"]}
for d in dirs.values():
    os.makedirs(d, exist_ok=True)

# Revision in the file name: an unchanged page rewrites the same file, and Auto Loader ignores overwrites
# (cloudFiles.allowOverwrites defaults to false), so reruns don't duplicate rows.
with open(f"{dirs['api_json']}/sections_rev{revid}.json", "w", encoding="utf-8") as f:
    json.dump(meta, f, ensure_ascii=False)          # one JSON object on one line
with open(f"{dirs['raw_html']}/current_programming_rev{revid}.html", "w", encoding="utf-8") as f:
    f.write(parsed["text"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parse the tables into records  **[prepares a JSON landing zone for Auto Loader, 2.3]**

# COMMAND ----------

soup = BeautifulSoup(parsed["text"], "html.parser")   # built-in parser; pd.read_html still uses lxml
for tag in soup.select("sup.reference, .mw-editsection, [style*='display:none']"):
    tag.decompose()                                  # footnotes [1], edit links, hidden sort keys


def clean_col(col):
    if isinstance(col, tuple):                       # two-row headers
        col = " ".join(dict.fromkeys(str(x) for x in col if not str(x).startswith("Unnamed")))
    return re.sub(r"[^0-9a-z]+", "_", str(col).lower()).strip("_")


def dedupe(cols):
    """Two headers can clean to the same name (e.g. 'Seasons' twice): make them seasons, seasons_2."""
    seen, out = {}, []
    for c in cols:
        c = c or "col"
        if c in ("category", "subcategory"):          # names this notebook adds itself
            c = f"src_{c}"
        n = seen.get(c, 0)
        seen[c] = n + 1
        out.append(c if n == 0 else f"{c}_{n + 1}")
    return out


frames, category, subcategory = [], None, None
for el in soup.find_all(["h3", "h4", "table"]):
    if el.name == "h3":
        category, subcategory = el.get_text(strip=True), None
    elif el.name == "h4":
        subcategory = el.get_text(strip=True)
    elif "wikitable" in (el.get("class") or []) and category not in EXCLUDE_SUBSECTIONS:
        df = pd.read_html(StringIO(str(el)))[0]
        df.columns = dedupe([clean_col(c) for c in df.columns])
        for c in df.select_dtypes("number").columns:  # keep 2023 as "2023", not "2023.0"
            if (df[c].dropna() % 1 == 0).all():
                df[c] = df[c].astype("Int64")
        df.insert(0, "category", category)
        df.insert(1, "subcategory", subcategory)
        frames.append(df)

if not frames:
    raise ValueError("No tables under Current programming - check raw_html; the page layout may have changed.")

records = pd.concat(frames, ignore_index=True).astype("string")   # all text in bronze; types in silver
records["source_url"] = f"https://en.wikipedia.org/wiki/{PAGE}"
records["source_revid"] = str(revid)
records["extracted_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

out = f"{dirs['records']}/current_programming_rev{revid}.jsonl"
records.to_json(out, orient="records", lines=True, force_ascii=False)
print(f"{len(frames)} tables, {len(records)} rows -> {out}")
print(records["category"].value_counts().to_string())