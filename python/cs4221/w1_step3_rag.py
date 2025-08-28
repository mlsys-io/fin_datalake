# /opt/app/bin/w1_step3_rag.py
import os, json
import uuid
import requests
from pyspark.sql import SparkSession, types as T, functions as F
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -------- config from env --------
JDBC_URL  = os.environ["JDBC_URL"]
JDBC_USER = os.environ["JDBC_USER"]
JDBC_PASS = os.environ["JDBC_PASSWORD"]
SCHEMA    = os.environ.get("SCHEMA", "public")
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://ollama.ollama.svc.cluster.local:11434/api/generate")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.1")
WINDOW_DAYS = int(os.environ.get("CTX_WINDOW_DAYS", "14"))
TOPK_NEWS   = int(os.environ.get("TOPK_NEWS", "8"))
TOPK_SEC    = int(os.environ.get("TOPK_SEC", "4"))
REQUEST_TIMEOUT = int(os.environ.get("OLLAMA_TIMEOUT_SEC", "120"))
PARTITION_SIZE  = int(os.environ.get("PARTITION_SIZE", "50"))  # rows per partition
EMBED_MODEL = os.environ.get("EMBED_MODEL", "nomic-embed-text")
EMBED_URL   = os.environ.get("OLLAMA_EMBED_URL", "http://ollama.ollama.svc.cluster.local:11434/api/embed")


_qsess = None
def _q_session():
    global _qsess
    if _qsess is None:
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        s = requests.Session()
        retry = Retry(total=3, backoff_factor=0.6,
                      status_forcelist=(429,500,502,503,504),
                      allowed_methods=["POST"])
        s.mount("http://", HTTPAdapter(pool_connections=4, pool_maxsize=16, max_retries=retry))
        s.mount("https://", HTTPAdapter(pool_connections=4, pool_maxsize=16, max_retries=retry))
        _qsess = s
    return _qsess

def _embed_query(text: str):
    if text is None or not text.strip(): return None
    s = _q_session()
    r = s.post(EMBED_URL, json={
        "model": EMBED_MODEL, 
        "input": text, 
        "keep_alive": "30m"
    }, timeout=120)
    r.raise_for_status()
    vec = (r.json().get("embeddings") or [None])[0]
    return [float(x) for x in vec] if vec else None

embed_query_udf = F.udf(_embed_query, T.ArrayType(T.FloatType()))

spark = (SparkSession.builder
         .appName("W1-Step3-RAG")
         .enableHiveSupport()
         .getOrCreate())

jdbc_opts = {"url": JDBC_URL, "driver":"org.postgresql.Driver",
             "user": JDBC_USER, "password": JDBC_PASS}

# Ensure target table exists in TSDB (Postgres DDL via JDBC)
def jdbcexec(sql):
    # Execute a single SQL statement via Spark driver JDBC
    spark._sc._jvm.java.sql.DriverManager.getConnection(
        JDBC_URL, JDBC_USER, JDBC_PASS
    ).createStatement().execute(sql)

def write_upsert(df, target_table, cols, key_cols, do_update=False):
    stage = f"{target_table}_stg_{uuid.uuid4().hex[:8]}"
    col_defs = ", ".join([f'"{c}" TEXT' for c in cols])
    jdbcexec(f'CREATE TABLE {SCHEMA}."{stage}" ({col_defs})')
    (df.select([F.col(c).cast("string").alias(c) for c in cols])
       .write.mode("append")
       .format("jdbc")
       .option("url", JDBC_URL)
       .option("dbtable", f'{SCHEMA}."{stage}"')
       .options(**jdbc_opts)
       .save())

    tgt_cols = ", ".join([f'"{c}"' for c in cols])
    casts = {
        "bucket_start": "TIMESTAMPTZ", 
        "change_ratio": "DOUBLE PRECISION",
        "avg_close": "DOUBLE PRECISION",
        "prev_avg_close": "DOUBLE PRECISION",
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
        INSERT INTO {SCHEMA}."{target_table}" ({tgt_cols})
        SELECT {select_casts} FROM {SCHEMA}."{stage}"
        ON CONFLICT ({conflict}) DO UPDATE SET {set_list}
        """
    else:
        merge_sql = f"""
        INSERT INTO {SCHEMA}."{target_table}" ({tgt_cols})
        SELECT {select_casts} FROM {SCHEMA}."{stage}"
        ON CONFLICT ({conflict}) DO NOTHING
        """
    jdbcexec(merge_sql)
    jdbcexec(f'DROP TABLE {SCHEMA}.{stage}')

jdbcexec(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.w1_explanations (
  symbol TEXT NOT NULL,
  bucket_start TIMESTAMPTZ NOT NULL,
  change_ratio DOUBLE PRECISION,
  avg_close DOUBLE PRECISION,
  prev_avg_close DOUBLE PRECISION,
  evidence JSONB,           -- collected docs (news + sec)
  answer   JSONB,           -- model output (structured JSON)
  model    TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (symbol, bucket_start)
);
""")

# Read changes (already significant in Step 2; further filter by date if you want)
changes = spark.read.format("jdbc").options(**jdbc_opts)\
    .option("dbtable", f"{SCHEMA}.w1_symbol_changes").load()\
    .repartition(max(1, int(PARTITION_SIZE)))

changes_q = (
  changes
  .withColumn(
      "query_text",
      F.concat_ws(" ",
        F.lit("Explain"), F.col("symbol"),
        F.lit("price movement around"),
        F.date_format(F.col("bucket_start"), "yyyy-MM-dd"),
        F.lit("; likely drivers: earnings, guidance, product, regulation, macro; top causes.")
      )
  )
  .withColumn("qvec", embed_query_udf(F.col("query_text")))
  .where(F.col("qvec").isNotNull())
  .withColumn("qvec_json", F.to_json("qvec"))
  .drop("qvec")
)

# Helper: fetch docs from TSDB for one (symbol, bucket_start)
def fetch_context(symbol, bucket_start, cur):
    # prefer vectors if any exist; else FTS
    # quick check: counts from embeddings
    cur.execute(f"SELECT EXISTS(SELECT 1 FROM {SCHEMA}.news_embeddings)")
    has_news_vec = cur.fetchone()[0]
    cur.execute(f"SELECT EXISTS(SELECT 1 FROM {SCHEMA}.sec_embeddings)")

    # time window
    cur.execute("SELECT %s::timestamptz - INTERVAL %s, %s::timestamptz + INTERVAL %s",
                (bucket_start, f"'{WINDOW_DAYS} days'",
                 bucket_start, f"'{WINDOW_DAYS} days'"))
    t_from, t_to = cur.fetchone()

    docs = {"news": [], "sec": []}

    if has_news_vec:
        # vector KNN by symbol + time (cosine or l2 depending on your index)
        # If you don't have a query embedding, approximate with recency + filter by symbol;
        # here we simply return top-K recent news around the window joined to symbol mapping.
        # If you later add query embeddings, ORDER BY e.embedding <-> :query_embedding.
        cur.execute(f"""
        SELECT a.id, a.ts, a.title, a.summary, a.source, a.url
        FROM {SCHEMA}.news_articles a
        JOIN {SCHEMA}.news_by_symbol s ON s.id = a.id
        WHERE s.symbol = %s AND a.ts BETWEEN %s AND %s
        ORDER BY a.ts DESC
        LIMIT %s
        """, (symbol, t_from, t_to, TOPK_NEWS))
        for r in cur.fetchall():
            docs["news"].append({"id": r[0], "ts": str(r[1]), "title": r[2],
                                 "summary": r[3], "source": r[4], "url": r[5]})
    else:
        # FTS + recency (simple, robust). ts_rank for relevance. :contentReference[oaicite:5]{index=5}
        cur.execute(f"""
        SELECT a.id, a.ts, a.title, a.summary, a.source, a.url,
               ts_rank(to_tsvector('english', coalesce(a.title,'')||' '||coalesce(a.summary,'')),
                       plainto_tsquery('english', %s)) AS rank
        FROM {SCHEMA}.news_articles a
        JOIN {SCHEMA}.news_by_symbol s ON s.id = a.id
        WHERE s.symbol = %s AND a.ts BETWEEN %s AND %s
        ORDER BY rank DESC, a.ts DESC
        LIMIT %s
        """, (symbol, symbol, t_from, t_to, TOPK_NEWS))
        for r in cur.fetchall():
            docs["news"].append({"id": r[0], "ts": str(r[1]), "title": r[2],
                                 "summary": r[3], "source": r[4], "url": r[5]})

    # SEC filings (no vector assumption; pick recent forms)
    cur.execute(f"""
    SELECT id, filed_ts, form, text_content
    FROM {SCHEMA}.sec_filings
    WHERE symbol = %s AND filed_ts BETWEEN %s AND %s
    ORDER BY filed_ts DESC
    LIMIT %s
    """, (symbol, t_from, t_to, TOPK_SEC))
    for r in cur.fetchall():
        docs["sec"].append({"id": r[0], "filed_ts": str(r[1]), "form": r[2],
                            "text": r[3][:5000] if r[3] else None})  # cap tokens

    return docs

def fetch_context_knn(cur, symbol, bucket_start, qvec_json, days=WINDOW_DAYS):
    cur.execute("SELECT %s::timestamptz - INTERVAL %s, %s::timestamptz + INTERVAL %s",
                (bucket_start, f"'{days} days'", bucket_start, f"'{days} days'"))
    t_from, t_to = cur.fetchone()

    docs = {"news": [], "sec": []}

    # NEWS via cosine KNN
    cur.execute(f"""
      SELECT a.id, a.ts, a.title, a.summary, a.source, a.url
      FROM {SCHEMA}.news_embeddings e
      JOIN {SCHEMA}.news_articles a ON a.id = e.id
      JOIN {SCHEMA}.news_by_symbol s ON s.id = a.id
      WHERE s.symbol = %s AND a.ts BETWEEN %s AND %s
      ORDER BY e.embedding <=> %s::vector
      LIMIT %s
    """, (symbol, t_from, t_to, qvec_json, TOPK_NEWS))
    for r in cur.fetchall():
        docs["news"].append({"id": r[0], "ts": str(r[1]), "title": r[2],
                             "summary": r[3], "source": r[4], "url": r[5]})

    # SEC via cosine KNN
    cur.execute(f"""
      SELECT f.id, f.filed_ts, f.form, f.text_content
      FROM {SCHEMA}.sec_embeddings e
      JOIN {SCHEMA}.sec_filings f ON f.id = e.id
      WHERE f.symbol = %s AND f.filed_ts BETWEEN %s AND %s
      ORDER BY e.embedding <=> %s::vector
      LIMIT %s
    """, (symbol, t_from, t_to, qvec_json, TOPK_SEC))
    for r in cur.fetchall():
        docs["sec"].append({"id": r[0], "filed_ts": str(r[1]), "form": r[2],
                            "text": (r[3] or "")[:5000]})
    return docs

# Build the prompt; ask for strict JSON (Ollama JSON mode). :contentReference[oaicite:6]{index=6}
def build_prompt(row, context_docs):
    instr = (
      "You are an equity analyst. Explain the most likely drivers of the price change.\n"
      "Use only the provided context (news + SEC snippets). If uncertain, say so.\n"
      "Return a JSON object with keys: summary, drivers[], risks[], sources[]."
    )
    facts = (
      f"SYMBOL: {row['symbol']}\n"
      f"WINDOW_START: {row['bucket_start']}\n"
      f"AVG_CLOSE: {row['avg_close']}\n"
      f"PREV_AVG_CLOSE: {row['prev_avg_close']}\n"
      f"CHANGE_RATIO: {row['change_ratio']}\n"
    )
    ctx_lines = []
    for n in context_docs.get("news", []):
        ctx_lines.append(f"[NEWS] {n['ts']} {n['title']} :: {n.get('summary','')}")
    for s in context_docs.get("sec", []):
        ctx_lines.append(f"[SEC {s['form']}] {s['filed_ts']} :: { (s.get('text') or '') }")
    context_text = "\n".join(ctx_lines[:TOPK_NEWS + TOPK_SEC])

    prompt = (
      f"{instr}\n\n"
      f"FACTS:\n{facts}\n"
      f"CONTEXT:\n{context_text}\n\n"
      "Respond ONLY in valid JSON with keys {summary, drivers, risks, sources}."
    )
    return prompt

def call_ollama(prompt):
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "format": "json",
        "stream": False,
        "options": {"temperature": 0}  # more deterministic JSON. :contentReference[oaicite:7]{index=7}
    }
    r = requests.post(OLLAMA_URL, json=payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    # Ollama returns {response: "...", ...}; parse JSON inside response if needed
    txt = data.get("response","").strip()
    try:
        return json.loads(txt)
    except Exception:
        # as a fallback, wrap plain text
        return {"summary": txt, "drivers": [], "risks": [], "sources": []}

def make_session():
    s = requests.Session()
    retry = Retry(
        total=3, backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["POST","GET"]
    )
    s.mount("http://", HTTPAdapter(pool_connections=2, pool_maxsize=8, max_retries=retry))
    s.mount("https://", HTTPAdapter(pool_connections=2, pool_maxsize=8, max_retries=retry))
    return s

def call_ollama_with_session(sess, prompt):
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "format": "json",
        "stream": False,
        "keep_alive": "30m",
        "options": {"temperature": 0}
    }
    r = sess.post(OLLAMA_URL, json=payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    out = r.json().get("response","").strip()
    return json.loads(out) if out.startswith("{") else {"summary": out, "drivers": [], "risks": [], "sources": []}

# Build result rows as a DataFrame with TEXT JSON columns
def explain_partition(rows):
    import psycopg2, json
    sess = make_session()
    conn = psycopg2.connect(JDBC_URL.replace("jdbc:postgresql://","postgresql://"),
                            user=JDBC_USER, password=JDBC_PASS)
    cur = conn.cursor()
    for row in rows:
        docs = fetch_context_knn(cur, row["symbol"], row["bucket_start"], row["qvec_json"], days=WINDOW_DAYS)
        prompt = build_prompt(row, docs)
        ans = call_ollama_with_session(sess, prompt)
        yield (row["symbol"], row["bucket_start"], float(row["change_ratio"]) if row["change_ratio"] else None,
               float(row["avg_close"]) if row["avg_close"] else None,
               float(row["prev_avg_close"]) if row["prev_avg_close"] else None,
               json.dumps(docs), json.dumps(ans), OLLAMA_MODEL)

schema = T.StructType([
    T.StructField("symbol", T.StringType()),
    T.StructField("bucket_start", T.TimestampType()),
    T.StructField("change_ratio", T.DoubleType()),
    T.StructField("avg_close", T.DoubleType()),
    T.StructField("prev_avg_close", T.DoubleType()),
    T.StructField("evidence_json", T.StringType()),  # TEXT
    T.StructField("answer_json",   T.StringType()),  # TEXT
    T.StructField("model",         T.StringType()),
])

coalesce_n = int(os.environ.get("RAG_COALESCE", "2"))
explain_df = spark.createDataFrame(
    changes_q.coalesce(coalesce_n).rdd.mapPartitions(explain_partition), schema=schema
)

# Staging table managed by your upsert helper
explain_stg = f"{SCHEMA}.w1_explanations_stg"
jdbcexec(f"""
CREATE TABLE IF NOT EXISTS {explain_stg} (
  symbol TEXT NOT NULL,
  bucket_start TIMESTAMPTZ NOT NULL,
  change_ratio DOUBLE PRECISION,
  avg_close DOUBLE PRECISION,
  prev_avg_close DOUBLE PRECISION,
  evidence_json TEXT,
  answer_json TEXT,
  model TEXT,
  PRIMARY KEY (symbol, bucket_start)
);
""")

# Reuse your helper to save/merge staging rows
write_upsert(
    explain_df.select("symbol","bucket_start","change_ratio","avg_close","prev_avg_close","evidence_json","answer_json","model"),
    "w1_explanations_stg",
    ["symbol","bucket_start","change_ratio","avg_close","prev_avg_close","evidence_json","answer_json","model"],
    ["symbol","bucket_start"]
)

# Final cast + upsert from staging → real table with jsonb
jdbcexec(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.w1_explanations (
  symbol TEXT NOT NULL,
  bucket_start TIMESTAMPTZ NOT NULL,
  change_ratio DOUBLE PRECISION,
  avg_close DOUBLE PRECISION,
  prev_avg_close DOUBLE PRECISION,
  evidence JSONB,
  answer   JSONB,
  model    TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (symbol, bucket_start)
);

INSERT INTO {SCHEMA}.w1_explanations
  (symbol, bucket_start, change_ratio, avg_close, prev_avg_close, evidence, answer, model)
SELECT symbol, bucket_start, change_ratio, avg_close, prev_avg_close,
       NULLIF(evidence_json,'')::jsonb, NULLIF(answer_json,'')::jsonb, model
FROM {explain_stg}
ON CONFLICT (symbol, bucket_start) DO UPDATE SET
  change_ratio   = EXCLUDED.change_ratio,
  avg_close      = EXCLUDED.avg_close,
  prev_avg_close = EXCLUDED.prev_avg_close,
  evidence       = EXCLUDED.evidence,
  answer         = EXCLUDED.answer,
  model          = EXCLUDED.model;

DROP TABLE {explain_stg};
""")

print("Step 3 complete (rows upserted into public.w1_explanations).")