import os
import uuid
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder \
    .appName("W1-Step1-ETL-TSDB") \
    .enableHiveSupport() \
    .getOrCreate()

HIVE_DB_SILVER = os.getenv("HIVE_DB_SILVER", "cs4221_silver")
TSDB_URL       = os.getenv("TSDB_JDBC_URL", "jdbc:postgresql://timescaledb.default.svc:5432/tsdb")
TSDB_USER      = os.getenv("TSDB_USER", "postgres")
TSDB_PASSWORD  = os.getenv("TSDB_PASSWORD", "postgres")
TSDB_SCHEMA    = os.getenv("TSDB_SCHEMA", "public")
EMBED_MODEL = os.getenv("EMBED_MODEL", "nomic-embed-text")
EMBED_DIM   = int(os.getenv("EMBED_DIM", "768"))
EMBED_URL = os.getenv("OLLAMA_EMBED_URL", "http://ollama.ollama.svc.cluster.local:11434/api/embed")

_session = None
def _get_session():
    global _session
    if _session is None:
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        s = requests.Session()
        retry = Retry(total=3, backoff_factor=0.5,
                      status_forcelist=(429, 500, 502, 503, 504),
                      allowed_methods=["POST"])
        s.mount("http://", HTTPAdapter(pool_connections=4, pool_maxsize=16, max_retries=retry))
        s.mount("https://", HTTPAdapter(pool_connections=4, pool_maxsize=16, max_retries=retry))
        _session = s
    return _session

def _embed_text(text: str):
    if not text or not text.strip():
        return None
    s = _get_session()  # your cached requests.Session
    r = s.post(EMBED_URL, json={
        "model": EMBED_MODEL, 
        "input": text,
        "keep_alive": "30m"
    }, timeout=120)
    r.raise_for_status()
    data = r.json()
    # /api/embed returns "embeddings": [[...]] even for a single input
    vec = (data.get("embeddings") or [None])[0]
    return [float(x) for x in vec] if vec else None

embed_text_udf = F.udf(_embed_text, T.ArrayType(T.FloatType()))

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
        "id": "BIGINT", 
        "ts": "TIMESTAMPTZ", 
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
    jdbcexec(f'DROP TABLE {TSDB_SCHEMA}.{stage}')

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
try: 
    jdbcexec("CREATE EXTENSION IF NOT EXISTS vector")
    print("vector extension available")
except: 
    print("vector extension not available, using float8[] instead")
    has_vector = False
if has_vector:
    jdbcexec(f"""
    CREATE TABLE IF NOT EXISTS {TSDB_SCHEMA}.news_embeddings (
    id BIGINT PRIMARY KEY REFERENCES {TSDB_SCHEMA}.news_articles(id) ON DELETE CASCADE,
    embedding vector({EMBED_DIM})
    );

    CREATE TABLE IF NOT EXISTS {TSDB_SCHEMA}.sec_embeddings (
    id BIGINT PRIMARY KEY REFERENCES {TSDB_SCHEMA}.sec_filings(id) ON DELETE CASCADE,
    embedding vector({EMBED_DIM})
    );

    CREATE INDEX IF NOT EXISTS news_embedding_hnsw
    ON {TSDB_SCHEMA}.news_embeddings USING hnsw (embedding vector_cosine_ops);

    CREATE INDEX IF NOT EXISTS sec_embedding_hnsw
    ON {TSDB_SCHEMA}.sec_embeddings USING hnsw (embedding vector_cosine_ops);
    """)
else:
    jdbcexec(f"""
    CREATE TABLE IF NOT EXISTS {TSDB_SCHEMA}.news_embeddings (
      id BIGINT PRIMARY KEY REFERENCES {TSDB_SCHEMA}.news_articles(id) ON DELETE CASCADE,
      embedding float8[]
    );
    """)
    jdbcexec(f"""
    CREATE TABLE IF NOT EXISTS {TSDB_SCHEMA}.sec_embeddings (
      id BIGINT PRIMARY KEY REFERENCES {TSDB_SCHEMA}.sec_filings(id) ON DELETE CASCADE,
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
sec_filings_df = spark.table(f"{HIVE_DB_SILVER}.sec_filings_silver").select(
    F.col("SYMBOL").alias("symbol"),
    F.col("CIK").alias("id"),
    F.col("filed_ts"),
    F.col("form"),
    F.col("text_content"),
)

# Prepare DFs
articles_df = news.dropDuplicates(["id"]).drop("symbol")
pairs_df = news.select("id", "symbol").dropDuplicates(["id", "symbol"])
profiles_df = profiles_src.dropDuplicates(["symbol"])

news_embed_df = (
    articles_df
    .select(
        F.col("id").cast("long"),
        F.concat_ws(" ",
            F.coalesce("title", F.lit("")),
            F.coalesce("summary", F.lit("")),
            F.coalesce("content", F.lit(""))
        ).alias("text_for_embed")
    )
    .withColumn("embedding", embed_text_udf(F.col("text_for_embed")))
    .where(F.col("embedding").isNotNull())
    .select("id", F.to_json("embedding").alias("embedding_json"))
)

# Use your write_upsert to save to a staging table with simple TEXT
news_stg = f"{TSDB_SCHEMA}.news_embeddings_stg"
jdbcexec(f"CREATE TABLE IF NOT EXISTS {news_stg} (id BIGINT PRIMARY KEY, embedding_json TEXT)")
write_upsert(news_embed_df, "news_embeddings_stg", ["id","embedding_json"], ["id"])

# Merge staging → real pgvector table (cast ::vector), then clear staging
jdbcexec(f"""
INSERT INTO {TSDB_SCHEMA}.news_embeddings (id, embedding)
SELECT id, embedding_json::vector
FROM {news_stg}
ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding;

DROP TABLE {news_stg};
""")

sec_embed_df = (
    sec_filings_df
    .select(
        F.col("id").cast("long"),
        F.concat_ws(" ",
            F.coalesce("form", F.lit("")),
            F.coalesce(F.substring("text_content", 1, 8000), F.lit(""))
        ).alias("text_for_embed")
    )
    .withColumn("embedding", embed_text_udf(F.col("text_for_embed")))
    .where(F.col("embedding").isNotNull())
    .select("id", F.to_json("embedding").alias("embedding_json"))
)

sec_stg = f"{TSDB_SCHEMA}.sec_embeddings_stg"
jdbcexec(f"CREATE TABLE IF NOT EXISTS {sec_stg} (id BIGINT PRIMARY KEY, embedding_json TEXT)")
write_upsert(sec_embed_df, "sec_embeddings_stg", ["id","embedding_json"], ["id"])

jdbcexec(f"""
INSERT INTO {TSDB_SCHEMA}.sec_embeddings (id, embedding)
SELECT id, embedding_json::vector
FROM {sec_stg}
ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding;

DROP TABLE {sec_stg};
""")

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
