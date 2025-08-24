# -*- coding: utf-8 -*-
"""
Bronze (raw → Delta), schema-less/minimal

- No explicit schemas (CSV: inferSchema; JSON: inferred; text: raw)
- Only adds _source_path and _ingest_time
- Writes Delta datasets
- (Optional) Registers Delta tables in Hive
"""

import os
from pyspark.sql import SparkSession, functions as F

# --------------------------- CONFIG ---------------------------
def s3p(bucket_name, *parts: str) -> str:
    return f"s3a://{bucket_name}/" + "/".join(p.strip("/") for p in parts if p)

RAW_BUCKET = os.getenv("RAW_BUCKET", "raw-dataset")
RAW_PREFIX = os.getenv("RAW_PREFIX", "")

OHLC_DIR = s3p(RAW_BUCKET, RAW_PREFIX, "OHLC")
NEWS_DIR = s3p(RAW_BUCKET, RAW_PREFIX, "News")
SEC_DIR  = s3p(RAW_BUCKET, RAW_PREFIX, "SEC-Filings")
PROF_DIR = s3p(RAW_BUCKET, RAW_PREFIX, "profile_estimate/profile.json")
HEE_DIR  = s3p(RAW_BUCKET, RAW_PREFIX, "profile_estimate/historical_earning_estimates.json")

DELTA_ROOT = os.getenv("DELTA_ROOT", s3p("delta-lake", "cs4221/bronze"))
HIVE_DB    = os.getenv("HIVE_DB_BRONZE", "cs4221_bronze")
REGISTER_IN_HIVE = os.getenv("REGISTER_IN_HIVE", "true").lower() == "true"
WRITE_MODE = os.getenv("DELTA_WRITE_MODE", "overwrite")  # "append" for incremental loads

# --------------------------- SPARK ---------------------------
spark = (
    SparkSession.builder
    .appName("CS4221-Bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

if REGISTER_IN_HIVE:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")

ingest_time = F.current_timestamp()

def write_delta(df, path):
    print(f"Writing into {path} ...")
    (df.write
       .format("delta")
       .mode(WRITE_MODE)
       .save(path))
    print(f"Finished writing into {path}.")

# --------------------------- 1) OHLC (CSV.gz → Delta) ---------------------------
# Minimal: let Spark infer; no casting; keep raw columns exactly as read.
ohlc_raw = (
    spark.read.format("csv")
      .option("header", True)
      .option("recursiveFileLookup", True)
      .option("pathGlobFilter", "*.gz")
      .option("inferSchema", True)      # allow inference
      .option("samplingRatio", 1.0)     # stronger inference across all data
      .load(OHLC_DIR)
      .withColumn("_source_path", F.input_file_name())
      .withColumn("_ingest_time", ingest_time)
)
ohlc_delta_path = f"{DELTA_ROOT}/ohlc"
write_delta(ohlc_raw, ohlc_delta_path)

# --------------------------- 2) News JSON (news.json → Delta) ---------------------------
# Minimal: parse JSON with inference. Keep everything.
news_json_raw = (
    spark.read.format("json")
      .option("multiLine", True)          # pretty-printed JSON objects
      .option("recursiveFileLookup", True)
      .option("pathGlobFilter", "news.json")
      # TIP: if you want to be ultra-conservative re: types, uncomment the next line:
      # .option("primitivesAsStrings", True)
      .load(NEWS_DIR)
      .withColumn("_source_path", F.input_file_name())
      .withColumn("_ingest_time", ingest_time)
)
news_json_delta_path = f"{DELTA_ROOT}/news_json"
write_delta(news_json_raw, news_json_delta_path)

# --------------------------- 3) News HTML (*.html as raw text → Delta) ---------------------------
# Minimal: store raw HTML as a single "value" column from text source.
news_html_raw = (
    spark.read.format("text")
      .option("recursiveFileLookup", True)
      .option("pathGlobFilter", "*.html")
      .load(NEWS_DIR)
      .withColumn("_source_path", F.input_file_name())
      .withColumn("_ingest_time", ingest_time)
)
news_html_delta_path = f"{DELTA_ROOT}/news_html"
write_delta(news_html_raw, news_html_delta_path)

# --------------------------- 4) Profiles (map JSON → Delta, inferred) ---------------------------
profiles_bronze = (
    spark.read.format("json")
      .option("multiLine", True)
      .load(PROF_DIR)
      .withColumn("_source_path", F.input_file_name())
      .withColumn("_ingest_time", ingest_time)
)
profiles_delta_path = f"{DELTA_ROOT}/profiles_map"
write_delta(profiles_bronze, profiles_delta_path)

# --------------------------- 5) Historical Earning Estimates (map JSON → Delta, inferred) ---------------------------
hee_bronze = (
    spark.read.format("json")
      .option("multiLine", True)
      .load(HEE_DIR)
      .withColumn("_source_path", F.input_file_name())
      .withColumn("_ingest_time", ingest_time)
)
hee_delta_path = f"{DELTA_ROOT}/historical_earning_estimates_map"
write_delta(hee_bronze, hee_delta_path)

# --------------------------- 6) SEC Filings (XML payloads saved as .txt.gz → Delta) ---------------------------
# Minimal: store raw text blobs; XML parsing is deferred to ETL (silver).
sec_bronze = (
    spark.read.format("text")
      .option("recursiveFileLookup", True)
      .option("pathGlobFilter", "*.gz")
      .load(SEC_DIR)
      .withColumn("_source_path", F.input_file_name())
      .withColumn("_ingest_time", ingest_time)
)
sec_delta_path = f"{DELTA_ROOT}/sec_filings_text"
write_delta(sec_bronze, sec_delta_path)

# --------------------------- (Optional) Register in Hive as Delta tables ---------------------------
if REGISTER_IN_HIVE:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {HIVE_DB}.ohlc_bronze                         USING DELTA LOCATION '{ohlc_delta_path}'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {HIVE_DB}.news_json_bronze                    USING DELTA LOCATION '{news_json_delta_path}'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {HIVE_DB}.news_html_bronze                    USING DELTA LOCATION '{news_html_delta_path}'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {HIVE_DB}.profiles_map_bronze                 USING DELTA LOCATION '{profiles_delta_path}'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {HIVE_DB}.historical_earning_estimates_bronze USING DELTA LOCATION '{hee_delta_path}'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {HIVE_DB}.sec_filings_text_bronze             USING DELTA LOCATION '{sec_delta_path}'")

    # Make them visible immediately
    for t in [
        "ohlc_bronze", "news_json_bronze", "news_html_bronze",
        "profiles_map_bronze", "historical_earning_estimates_bronze",
        "sec_filings_text_bronze"
    ]:
        spark.catalog.refreshTable(f"{HIVE_DB}.{t}")

print("Bronze (schemaless) Delta re-store complete.")