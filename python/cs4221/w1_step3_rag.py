# /opt/app/bin/w1_step3_rag.py
import os, json
import requests
from pyspark.sql import SparkSession, types as T

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

spark = (SparkSession.builder
         .appName("W1-Step3-RAG")
         .enableHiveSupport()
         .getOrCreate())

jdbc_opts = {"url": JDBC_URL, "driver":"org.postgresql.Driver",
             "user": JDBC_USER, "password": JDBC_PASS}

def table_exists(name):
    q = f"""SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema = %s AND table_name = %s
            )"""
    return spark.read.format("jdbc").options(**jdbc_opts)\
        .option("dbtable", f"({q}) t")\
        .option("preparedStatement", "true")\
        .option("queryTimeout", "30")\
        .load()\
        .selectExpr("exists as ok").first()["ok"]

# Ensure target table exists in TSDB (Postgres DDL via JDBC)
def jdbcexec(sql):
    # Execute a single SQL statement via Spark driver JDBC
    spark._sc._jvm.java.sql.DriverManager.getConnection(
        JDBC_URL, JDBC_USER, JDBC_PASS
    ).createStatement().execute(sql)

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

# Helper: fetch docs from TSDB for one (symbol, bucket_start)
def fetch_context(symbol, bucket_start, cur):
    # prefer vectors if any exist; else FTS
    # quick check: counts from embeddings
    cur.execute(f"SELECT EXISTS(SELECT 1 FROM {SCHEMA}.news_embeddings)")
    has_news_vec = cur.fetchone()[0]
    cur.execute(f"SELECT EXISTS(SELECT 1 FROM {SCHEMA}.sec_embeddings)")
    has_sec_vec = cur.fetchone()[0]

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

# UDF-ish execution per partition (fewer HTTP sessions). :contentReference[oaicite:8]{index=8}
def explain_partition(rows):
    import psycopg2
    conn = psycopg2.connect(JDBC_URL.replace("jdbc:postgresql://","postgresql://"),
                            user=JDBC_USER, password=JDBC_PASS)
    cur = conn.cursor()
    out = []
    for row in rows:
        ctx = fetch_context(row["symbol"], row["bucket_start"], cur)
        prompt = build_prompt(row, ctx)
        ans = call_ollama(prompt)
        out.append((
            row["symbol"], row["bucket_start"], row["change_ratio"],
            row["avg_close"], row["prev_avg_close"],
            json.dumps(ctx), json.dumps(ans), OLLAMA_MODEL
        ))
    cur.close(); conn.close()
    return iter(out)

schema = T.StructType([
    T.StructField("symbol", T.StringType()),
    T.StructField("bucket_start", T.TimestampType()),
    T.StructField("change_ratio", T.DoubleType()),
    T.StructField("avg_close", T.DoubleType()),
    T.StructField("prev_avg_close", T.DoubleType()),
    T.StructField("evidence", T.StringType()),
    T.StructField("answer", T.StringType()),
    T.StructField("model", T.StringType()),
])

result_df = spark.createDataFrame(
    changes.rdd.mapPartitions(explain_partition), schema=schema
)

# ---------- replace everything below where you build result_df ----------

UPSERT_SQL = f"""
INSERT INTO {SCHEMA}.w1_explanations
  (symbol, bucket_start, change_ratio, avg_close, prev_avg_close, evidence, answer, model)
VALUES
  (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
ON CONFLICT (symbol, bucket_start) DO UPDATE SET
  change_ratio   = EXCLUDED.change_ratio,
  avg_close      = EXCLUDED.avg_close,
  prev_avg_close = EXCLUDED.prev_avg_close,
  evidence       = EXCLUDED.evidence,
  answer         = EXCLUDED.answer,
  model          = EXCLUDED.model
"""

def upsert_partition(rows):
    import psycopg2, json as _json
    conn = psycopg2.connect(
        JDBC_URL.replace("jdbc:postgresql://","postgresql://"),
        user=JDBC_USER, password=JDBC_PASS
    )
    conn.autocommit = False
    cur = conn.cursor()
    batch = 0
    try:
        for row in rows:
            # reuse our retrieval + LLM calls
            ctx = fetch_context(row["symbol"], row["bucket_start"], cur)
            prompt = build_prompt(row, ctx)
            ans = call_ollama(prompt)

            cur.execute(
                UPSERT_SQL,
                (
                    row["symbol"],
                    row["bucket_start"],
                    float(row["change_ratio"]) if row["change_ratio"] is not None else None,
                    float(row["avg_close"]) if row["avg_close"] is not None else None,
                    float(row["prev_avg_close"]) if row["prev_avg_close"] is not None else None,
                    _json.dumps(ctx),             # evidence JSON -> text then ::jsonb
                    _json.dumps(ans),             # answer   JSON -> text then ::jsonb
                    OLLAMA_MODEL,
                )
            )
            batch += 1
            if batch % 50 == 0:   # commit every 50 rows (tune as you like)
                conn.commit()
        if batch % 50 != 0:
            conn.commit()
    finally:
        try:
            cur.close()
        except: pass
        conn.close()

# Limit parallelism if you were overwhelming Ollama:
# e.g., only a few partitions at a time
changes = changes.coalesce(int(os.environ.get("RAG_COALESCE", "2")))

# Trigger execution with side-effects:
changes.rdd.foreachPartition(upsert_partition)

print("Step 3 complete (rows upserted into public.w1_explanations).")