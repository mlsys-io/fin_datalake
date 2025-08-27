import os
import uuid
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder \
    .appName("W1-Step1-ETL-TSDB") \
    .enableHiveSupport() \
    .getOrCreate()

HIVE_DB_SILVER = os.getenv("HIVE_DB_SILVER", "cs4221_silver")
TSDB_URL       = os.getenv("TSDB_JDBC_URL", "jdbc:postgresql://timescaledb.default.svc:5432/tsdb")
TSDB_USER      = os.getenv("TSDB_USER", "postgres")
TSDB_PASSWORD  = os.getenv("TSDB_PASSWORD", "postgres")
TSDB_SCHEMA    = os.getenv("TSDB_SCHEMA", "public")
EMBED_DIM      = int(os.getenv("EMBED_DIM", "768"))

jdbc_props = {
    "user": TSDB_USER,
    "password": TSDB_PASSWORD,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",
}

def jdbcexec(sql: str):
    jvm = spark._sc._jvm
    DriverManager = jvm.java.sql.DriverManager
    conn = DriverManager.getConnection(TSDB_URL, TSDB_USER, TSDB_PASSWORD)
    try:
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
        conn.commit()
    finally:
        conn.close()

def write_direct(df, table):
    (df.write
       .mode("append")
       .format("jdbc")
       .option("url", TSDB_URL)
       .option("dbtable", f"{TSDB_SCHEMA}.{table}")
       .options(**jdbc_props)
       .save())

def write_upsert(df, target_table, cols, key_cols, do_update=False):
    stage = f"{target_table}_stg_{uuid.uuid4().hex[:8]}"
    col_defs = ", ".join([f'"{c}" TEXT' for c in cols])
    jdbcexec(f'CREATE TABLE {TSDB_SCHEMA}."{stage}" ({col_defs})')
    (df.select([F.col(c).cast("string").alias(c) for c in cols])
       .write.mode("append")
       .format("jdbc")
       .option("url", TSDB_URL)
       .option("dbtable", f'{TSDB_SCHEMA}."{stage}"')
       .options(**jdbc_props)
       .save())

    tgt_cols = ", ".join([f'"{c}"' for c in cols])
    casts = {
        "id": "BIGINT",          # <-- add this
        "ts": "TIMESTAMPTZ",     # <-- and this
        "mktcap": "BIGINT"
    }
    cast_exprs = []
    for c in cols:
        typ = casts.get(c)
        if typ:
            cast_exprs.append(f"NULLIF(BTRIM(\"{c}\"), '')::{typ} AS \"{c}\"")
        else:
            cast_exprs.append(f"\"{c}\"")
    select_casts = ", ".join(cast_exprs)
    conflict = ", ".join([f'"{c}"' for c in key_cols])
    if do_update:
        set_list = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in key_cols])
        merge_sql = f"""
        INSERT INTO {TSDB_SCHEMA}."{target_table}" ({tgt_cols})
        SELECT {select_casts} FROM {TSDB_SCHEMA}."{stage}"
        ON CONFLICT ({conflict}) DO UPDATE SET {set_list}
        """
    else:
        merge_sql = f"""
        INSERT INTO {TSDB_SCHEMA}."{target_table}" ({tgt_cols})
        SELECT {select_casts} FROM {TSDB_SCHEMA}."{stage}"
        ON CONFLICT ({conflict}) DO NOTHING
        """
    jdbcexec(merge_sql)
    jdbcexec(f'DROP TABLE {TSDB_SCHEMA}."{stage}"')

# Setup tables
jdbcexec(f"CREATE SCHEMA IF NOT EXISTS {TSDB_SCHEMA}")
jdbcexec("CREATE EXTENSION IF NOT EXISTS timescaledb")

# News normalization
jdbcexec(f"""
CREATE TABLE IF NOT EXISTS {TSDB_SCHEMA}.news_articles (
  id BIGINT PRIMARY KEY,
  ts TIMESTAMPTZ, title TEXT, summary TEXT, content TEXT, url TEXT, source TEXT
);
""")
jdbcexec(f"""
CREATE INDEX IF NOT EXISTS news_articles_fts_idx
ON {TSDB_SCHEMA}.news_articles
USING GIN (to_tsvector('english', coalesce(title,'')||' '||coalesce(content,'')));
""")
jdbcexec(f"CREATE INDEX IF NOT EXISTS news_articles_ts_idx ON {TSDB_SCHEMA}.news_articles (ts DESC);")

jdbcexec(f"""
CREATE TABLE IF NOT EXISTS {TSDB_SCHEMA}.news_by_symbol (
  id BIGINT NOT NULL,
  symbol TEXT NOT NULL,
  PRIMARY KEY (id, symbol),
  FOREIGN KEY (id) REFERENCES {TSDB_SCHEMA}.news_articles(id) ON DELETE CASCADE
);
""")
jdbcexec(f"CREATE INDEX IF NOT EXISTS news_by_symbol_symbol_idx ON {TSDB_SCHEMA}.news_by_symbol (symbol);")

# Profiles
jdbcexec(f"""
CREATE TABLE IF NOT EXISTS {TSDB_SCHEMA}.profiles (
  symbol TEXT PRIMARY KEY,
  company_name TEXT, industry TEXT, sector TEXT, mktcap BIGINT
);
""")
jdbcexec(f"CREATE INDEX IF NOT EXISTS profiles_industry_idx ON {TSDB_SCHEMA}.profiles (industry);")

# RAG embeddings
has_vector = True
try: jdbcexec("CREATE EXTENSION IF NOT EXISTS vector")
except: has_vector = False
if has_vector:
    jdbcexec(f"""
    CREATE TABLE IF NOT EXISTS {TSDB_SCHEMA}.news_embeddings (
      doc_id BIGINT PRIMARY KEY REFERENCES {TSDB_SCHEMA}.news_articles(id) ON DELETE CASCADE,
      embedding VECTOR({EMBED_DIM})
    );
    """)
else:
    jdbcexec(f"""
    CREATE TABLE IF NOT EXISTS {TSDB_SCHEMA}.news_embeddings (
      doc_id BIGINT PRIMARY KEY REFERENCES {TSDB_SCHEMA}.news_articles(id) ON DELETE CASCADE,
      embedding float8[]
    );
    """)

# Read silver tables
news = spark.table(f"{HIVE_DB_SILVER}.news_silver").select(
    F.col("id").cast("long").alias("id"),
    F.col("SYMBOL").alias("symbol"),
    F.col("timestamp").alias("ts"),
    "title", "summary", "content", "url", "source"
)
profiles_src = spark.table(f"{HIVE_DB_SILVER}.profiles_silver").select(
    F.col("SYMBOL").alias("symbol"),
    F.col("companyName").alias("company_name"),
    F.col("industry"), F.col("sector"),
    F.col("mktCap").cast("long").alias("mktcap")
)

# Prepare DFs
articles_df = news.dropDuplicates(["id"]).drop("symbol")
pairs_df = news.select("id", "symbol").dropDuplicates(["id", "symbol"])
profiles_df = profiles_src.dropDuplicates(["symbol"])

# Write to TSDB
write_upsert(articles_df, "news_articles", ["id","ts","title","summary","content","url","source"], ["id"])
write_upsert(pairs_df, "news_by_symbol", ["id","symbol"], ["id","symbol"])
write_upsert(profiles_df, "profiles", ["symbol","company_name","industry","sector","mktcap"], ["symbol"], do_update=True)

# Planner stats
jdbcexec(f"ANALYZE {TSDB_SCHEMA}.stock_ohlc")
jdbcexec(f"ANALYZE {TSDB_SCHEMA}.profiles")
jdbcexec(f"ANALYZE {TSDB_SCHEMA}.sec_filings")
jdbcexec(f"ANALYZE {TSDB_SCHEMA}.news_articles")
jdbcexec(f"ANALYZE {TSDB_SCHEMA}.news_by_symbol")

print("Step 1 complete: normalized news schema loaded, hypertables/indexes created, embeddings table ready.")
