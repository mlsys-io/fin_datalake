# -*- coding: utf-8 -*-
"""
Silver ETL (reads from Delta Bronze, not raw files)
- Transforms OHLC / News (JSON+HTML) / Profiles / Earning Estimates / SEC
- Registers Hive silver tables
- Loads curated tables to TimescaleDB (hypertables where applicable)
"""

import os
from pyspark.sql import SparkSession, functions as F, types as T
import pandas as pd

def s3p(bucket_name, *parts: str) -> str:
    return f"s3a://{bucket_name}/" + "/".join(p.strip("/") for p in parts)

# --------------------------- CONFIG ---------------------------
# Bronze Hive DB (created in your bronze script)
HIVE_DB_BRONZE = os.getenv("HIVE_DB_BRONZE", "cs4221_bronze")
# Silver Hive DB to create
HIVE_DB_SILVER = os.getenv("HIVE_DB_SILVER", "cs4221_silver")
DELTA_ROOT = os.getenv("DELTA_ROOT", s3p("delta-lake", "cs4221/silver"))

# If you didn't register bronze in Hive, replace .table() reads with .format("delta").load(<bronze delta path>)
BRONZE = {
    "ohlc":                           f"{HIVE_DB_BRONZE}.ohlc_bronze",
    "news_json":                      f"{HIVE_DB_BRONZE}.news_json_bronze",
    "news_html":                      f"{HIVE_DB_BRONZE}.news_html_bronze",
    "profiles_map":                   f"{HIVE_DB_BRONZE}.profiles_map_bronze",
    "historical_earning_estimates":   f"{HIVE_DB_BRONZE}.historical_earning_estimates_bronze",
    "sec_filings_text":               f"{HIVE_DB_BRONZE}.sec_filings_text_bronze",
}

spark = (
    SparkSession.builder
    .appName("CS4221-Silver-From-Delta-Bronze")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB_SILVER}")

# --------------------------- HELPERS ---------------------------
def apply_schema(df, schema: T.StructType):
    if not isinstance(schema, T.StructType):
        raise TypeError("apply_schema expects a StructType (row-shaped) schema")

    wanted = [c.name for c in schema]  # from your silver target schema
    cols = []
    for field in schema.fields:
        name = field.name
        if name in df.columns:
            cols.append(F.col(name).cast(field.dataType).alias(name))
        else:
            # allow missing columns (left as NULL)
            cols.append(F.lit(None).cast(field.dataType).alias(name))
    # optionally carry metadata columns if present
    for meta_col, meta_type in [("_source_path", T.StringType()), ("_ingest_time", T.TimestampType())]:
        if meta_col in df.columns and meta_col not in wanted:
            cols.append(F.col(meta_col).cast(meta_type).alias(meta_col))
    return df.select(cols)

def write_delta_hive(df, table_name, partitionBy=None, mode="overwrite"):
    """
    Writes df to Delta at DELTA_SILVER_ROOT/table_name and registers/refreshes an
    external Hive table {HIVE_DB_SILVER}.{table_name} USING DELTA LOCATION '<path>'
    """
    path = f"{DELTA_ROOT.rstrip('/')}/{table_name}"
    print(f"Writing into {path} ...")

    w = (
        df.write
          .format("delta")
          .mode(mode)
          # helpful for evolving schemas across runs
          .option("overwriteSchema", "true")
          .option("mergeSchema", "true")
    )
    if partitionBy:
        w = w.partitionBy(*partitionBy)

    # 1) write Delta files
    w.save(path)

    # 2) make sure DB exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB_SILVER}")

    # 3) create external table if missing, else just point/refresh
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HIVE_DB_SILVER}.{table_name}
        USING DELTA
        LOCATION '{path}'
    """)
    spark.catalog.refreshTable(f"{HIVE_DB_SILVER}.{table_name}")
    print(f"Finished writing into {path}.")

# Reconstruct a MapType column named "m" from a bronze JSON DataFrame that may have:
#  (a) an existing single MapType col, or
#  (b) many top-level columns (one per symbol key) due to schemaless read.
def ensure_map_column(df):
    # If there is exactly one MapType column, use it.
    map_cols = [c for c, t in df.dtypes if t.startswith("map<")]
    if len(map_cols) == 1:
        return df.select(F.col(map_cols[0]).alias("m"))
    # Else, build a map by concatenating key->array(struct...) pairs for all non-metadata columns.
    meta_cols = {"_source_path", "_ingest_time"}
    data_cols = [c for c in df.columns if c not in meta_cols]
    if not data_cols:
        raise ValueError("No data columns found to construct map from.")
    # Build map_from_arrays for each column: map(k -> col)
    pairs = [F.map_from_arrays(F.array(F.lit(c)), F.array(F.col(c))) for c in data_cols]
    m = F.map_concat(*pairs)
    return df.select(m.alias("m"))

# --------------------------- 1) OHLC (from bronze delta) ---------------------------
ohlc_schema = T.StructType([
    T.StructField("timestamp", T.LongType(), True),
    T.StructField("open",  T.DoubleType(), True),
    T.StructField("high",  T.DoubleType(), True),
    T.StructField("low",   T.DoubleType(), True),
    T.StructField("close", T.DoubleType(), True),
    T.StructField("volume",T.LongType(),   True),
])
ohlc_bz = apply_schema(spark.table(BRONZE["ohlc"]), ohlc_schema)

# bronze ohlc likely has raw types; cast/normalize now
ohlc = (
    ohlc_bz
      .select(
          F.regexp_extract(F.col("_source_path"), r"/OHLC/([^/]+)/", 1).alias("SYMBOL"),
          # raw unix seconds might be string; cast safely
          F.col("timestamp").alias("unix_ts"),
          "open","high","low","close","volume"
      )
      .withColumn("timestamp", F.try_to_timestamp(F.from_unixtime("unix_ts")))
      .withColumn("date", F.to_date("timestamp"))
      .drop("unix_ts")
)
write_delta_hive(ohlc, "ohlc_silver", partitionBy=["SYMBOL","date"])

# --------------------------- 2) NEWS (JSON + HTML from bronze delta) ---------------------------
news_json_schema = T.StructType([
    T.StructField("category", T.StringType(),  True),
    T.StructField("datetime", T.LongType(),    True),   # Unix epoch seconds
    T.StructField("headline", T.StringType(),  True),
    T.StructField("id",       T.LongType(), True),
    T.StructField("image",    T.StringType(),  True),   # full <url …>…</url> block
    T.StructField("related",  T.StringType(),  True),   # ticker symbol
    T.StructField("source",   T.StringType(),  True),
    T.StructField("summary",  T.StringType(),  True),
    T.StructField("url",      T.StringType(),  True)    # full <url …>…</url> block
])
news_json_bz = apply_schema(spark.table(BRONZE["news_json"]), news_json_schema)

news_json = (
    news_json_bz
      .withColumn("SYMBOL_DIR", F.regexp_extract(F.col("_source_path"), r"/News/([^/]+)/", 1))
      .withColumn("SYMBOL",
          F.explode_outer(
              F.when(F.col("related").isNotNull(), F.split(F.col("related"), r"\s*,\s*"))
               .otherwise(F.array(F.col("SYMBOL_DIR")))
          )
      )
      .withColumn("timestamp", F.to_timestamp(F.from_unixtime(F.col("datetime").cast("long"))))
      .drop("SYMBOL_DIR")
)

# HTML bronze: value (raw html), _source_path
import trafilatura
from bs4 import BeautifulSoup

@F.pandas_udf("string")
def extract_article_udf(html_series: pd.Series) -> pd.Series:
    out = []
    for html in html_series.fillna(""):
        if not html:
            out.append(None); continue
        try:
            content = trafilatura.extract(html, favor_precision=True, include_comments=False, include_tables=False)
        except Exception:
            content = None
        if not content:
            try:
                soup = BeautifulSoup(html, "lxml")
            except Exception:
                soup = BeautifulSoup(html, "html.parser")
            ps = soup.find_all("p")
            content = " ".join(p.get_text(" ", strip=True) for p in ps) if ps else None
        if content and len(content) > 20000:
            content = content[:20000]
        out.append(content)
    return pd.Series(out)

news_html_bz = spark.table(BRONZE["news_html"])
news_html = (
    news_html_bz
      .withColumn("SYMBOL", F.regexp_extract(F.col("_source_path"), r"/News/([^/]+)/", 1))
      .withColumn("id", F.regexp_extract(F.col("_source_path"), r"/news/(\d+)\.html$", 1).cast("long"))
      .withColumn("content", extract_article_udf(F.col("value")))
      .select("id","SYMBOL","content")
      .filter(F.col("id").isNotNull())
)

news = (
    news_json.alias("j")
      .join(news_html.alias("h"), on=["id","SYMBOL"], how="left")
      .withColumn("missing_html", F.col("h.content").isNull())
      .withColumn("content", F.coalesce(F.col("h.content"), F.col("j.summary")))
      .select(
          F.col("j.id").alias("id"),
          "SYMBOL",
          F.col("j.category").alias("category"),
          F.col("j.headline").alias("title"),
          F.col("j.source").alias("source"),
          F.col("j.summary").alias("summary"),
          F.col("j.timestamp").alias("timestamp"),
          F.col("j.url").alias("url"),
          F.col("j.image").alias("image"),
          "content",
          "missing_html"
      )
      .withColumn("date", F.to_date("timestamp"))
)
write_delta_hive(news, "news_silver", partitionBy=["SYMBOL","date"])

# --------------------------- 3) PROFILES (map JSON → rows) ---------------------------
profiles_bz = spark.table(BRONZE["profiles_map"])
profiles_m = ensure_map_column(profiles_bz)  # single col "m" MapType<string, array<struct>>

profiles_entries = profiles_m.select(F.explode(F.map_entries("m")).alias("kv"))
profiles = (
    profiles_entries
      .select(
          F.col("kv.key").alias("SYMBOL_KEY"),
          F.explode_outer("kv.value").alias("prof")
      )
      .select(
          F.coalesce(F.col("prof.symbol"), F.col("SYMBOL_KEY")).alias("SYMBOL"),
          F.col("prof.price").alias("price"),
          F.col("prof.beta").alias("beta"),
          F.col("prof.volAvg").alias("volAvg"),
          F.col("prof.mktCap").alias("mktCap"),
          F.col("prof.lastDiv").alias("lastDiv"),
          F.col("prof.range").alias("range"),
          F.col("prof.changes").alias("changes"),
          F.col("prof.companyName").alias("companyName"),
          F.col("prof.currency").alias("currency"),
          F.col("prof.cik").alias("cik"),
          F.col("prof.isin").alias("isin"),
          F.col("prof.cusip").alias("cusip"),
          F.col("prof.exchange").alias("exchange"),
          F.col("prof.exchangeShortName").alias("exchangeShortName"),
          F.col("prof.industry").alias("industry"),
          F.col("prof.website").alias("website"),
          F.col("prof.description").alias("description"),
          F.col("prof.ceo").alias("ceo"),
          F.col("prof.sector").alias("sector"),
          F.col("prof.country").alias("country"),
          F.col("prof.fullTimeEmployees").alias("fullTimeEmployees"),
          F.col("prof.phone").alias("phone"),
          F.col("prof.address").alias("address"),
          F.col("prof.city").alias("city"),
          F.col("prof.state").alias("state"),
          F.col("prof.zip").alias("zip"),
          F.col("prof.dcfDiff").alias("dcfDiff"),
          F.col("prof.dcf").alias("dcf"),
          F.col("prof.image").alias("image"),
          F.col("prof.ipoDate").alias("ipoDate"),
          F.col("prof.defaultImage").alias("defaultImage"),
          F.col("prof.isEtf").alias("isEtf"),
          F.col("prof.isActivelyTrading").alias("isActivelyTrading"),
          F.col("prof.isAdr").alias("isAdr"),
          F.col("prof.isFund").alias("isFund"),
      )
)
write_delta_hive(profiles, "profiles_silver")

# --------------------------- 4) Historical Earning Estimates (map JSON → rows) ---------------------------
hee_bz = spark.table(BRONZE["historical_earning_estimates"])
hee_m = ensure_map_column(hee_bz)

hee_entries = hee_m.select(F.explode(F.map_entries("m")).alias("kv"))
historical_earning_estimates = (
    hee_entries
      .select(
          F.col("kv.key").alias("SYMBOL_KEY"),
          F.explode_outer("kv.value").alias("est")
      )
      .select(
          F.coalesce(F.col("est.symbol"), F.col("SYMBOL_KEY")).alias("SYMBOL"),
          F.col("est.date").alias("date"),
          F.col("est.epsEstimated").alias("epsEstimated"),
          F.col("est.time").alias("time"),
          F.col("est.revenueEstimated").alias("revenueEstimated"),
          F.col("est.updatedFromDate").alias("updatedFromDate"),
          F.col("est.fiscalDateEnding").alias("fiscalDateEnding"),
      )
)
write_delta_hive(historical_earning_estimates, "historical_earning_estimates_silver", partitionBy=["SYMBOL"])

# --------------------------- 5) SEC Filings (text → parse XML fallback) ---------------------------
sec_bz = spark.table(BRONZE["sec_filings_text"])

# Try native XML if available (Spark XML), else from_xml fallback using text column
def parse_sec_from_text(df_text):
    filing_schema = T.StructType([
        T.StructField("form", T.StringType(), True),
        T.StructField("cik",  T.StringType(), True),
        T.StructField("filed_ts", T.TimestampType(), True),
        T.StructField("symbol", T.StringType(), True),
        T.StructField("document", T.StringType(), True),
        T.StructField("text", T.StringType(), True),
    ])
    return (
        df_text
          .withColumn("SYMBOL", F.regexp_extract(F.col("_source_path"), r"/SEC-Filings/([^/]+)/", 1))
          .select("SYMBOL", F.from_xml(F.col("value"), filing_schema).alias("fx"))
          .select(
              "SYMBOL",
              F.col("fx.form").alias("form"),
              F.col("fx.cik").alias("CIK"),
              F.col("fx.filed_ts").alias("filed_ts"),
              F.col("fx.document").alias("html_content"),
              F.col("fx.text").alias("text_content"),
          )
    )

# Prefer Spark XML reader only if available on cluster; else always use fallback above.
# Since bronze keeps raw text, we go straight to the fallback:
filings = parse_sec_from_text(sec_bz).select(
    F.coalesce("SYMBOL","symbol").alias("SYMBOL"),
    F.coalesce("CIK","cik").alias("CIK"),
    "form",
    F.try_to_timestamp(F.when(F.trim(F.col("filed_ts").cast("string")) == "", None)
                        .otherwise(F.col("filed_ts"))).alias("filed_ts"),
    "html_content","text_content"
)
filings_out = filings.withColumn("filing_year", F.year("filed_ts"))
write_delta_hive(filings_out, "sec_filings_silver", partitionBy=["SYMBOL","filing_year"])

print("Silver ETL complete: read from Delta bronze, wrote Hive silver & Delta silver.")
