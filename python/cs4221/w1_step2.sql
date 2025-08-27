-- ================================================
-- Workload 1 - Step 2 (Spark SQL -> write results into TimescaleDB)
-- ================================================

-- Parameters (passed via --hivevar or env)
SET SCHEMA=${SCHEMA};
SET START_DATE=${START_DATE};          -- e.g. 2014-01-01
SET WINDOW_DAYS=${WINDOW_DAYS};        -- 7 or 14
SET RATIO=${RATIO};                    -- 0.05, 0.10, 0.15
-- JDBC creds for TSDB
--   ${JDBC_URL}  ${JDBC_USER}  ${JDBC_PASSWORD}

-- ---------- Source: mount TSDB tables (JDBC) ----------
CREATE OR REPLACE TEMPORARY VIEW tsdb_stock_ohlc
USING org.apache.spark.sql.jdbc
OPTIONS (
  url      "${JDBC_URL}",
  driver   "org.postgresql.Driver",
  dbtable  "${SCHEMA}.stock_ohlc",
  user     "${JDBC_USER}",
  password "${JDBC_PASSWORD}"
);

CREATE OR REPLACE TEMPORARY VIEW tsdb_profiles
USING org.apache.spark.sql.jdbc
OPTIONS (
  url      "${JDBC_URL}",
  driver   "org.postgresql.Driver",
  dbtable  "${SCHEMA}.profiles",
  user     "${JDBC_USER}",
  password "${JDBC_PASSWORD}"
);

-- ---------- Compute & materialize as temp views ----------

-- View 1: per-symbol flags
CREATE OR REPLACE TEMP VIEW flagged AS
WITH params AS (
  SELECT
    CAST('${START_DATE}' AS TIMESTAMP)               AS start_ts,
    CAST(${WINDOW_DAYS} * 86400 AS BIGINT)          AS window_sec,
    CAST(${RATIO} AS DOUBLE)                        AS ratio_threshold
),
buckets AS (
  SELECT
    o.symbol,
    o.ts,
    o.close,
    p.start_ts,
    p.window_sec,
    p.ratio_threshold,
    CAST(unix_timestamp(o.ts) - unix_timestamp(p.start_ts) AS BIGINT) AS delta_sec
  FROM tsdb_stock_ohlc o
  CROSS JOIN params p
  WHERE o.ts >= p.start_ts
),
bucketed AS (
  SELECT
    symbol,
    CAST(FLOOR(delta_sec / window_sec) AS BIGINT) AS bucket_idx,
    to_timestamp(unix_timestamp(start_ts) + (FLOOR(delta_sec / window_sec) * window_sec)) AS bucket_start,
    close
  FROM buckets
),
avg_per_bucket AS (
  SELECT
    symbol,
    bucket_idx,
    bucket_start,
    AVG(close) AS avg_close
  FROM bucketed
  GROUP BY symbol, bucket_idx, bucket_start
),
change_vs_prev AS (
  SELECT
    symbol,
    bucket_idx,
    bucket_start,
    avg_close,
    LAG(avg_close) OVER (PARTITION BY symbol ORDER BY bucket_idx) AS prev_avg_close,
    CASE
      WHEN LAG(avg_close) OVER (PARTITION BY symbol ORDER BY bucket_idx) IS NULL THEN NULL
      WHEN LAG(avg_close) OVER (PARTITION BY symbol ORDER BY bucket_idx) = 0 THEN NULL
      ELSE (avg_close - LAG(avg_close) OVER (PARTITION BY symbol ORDER BY bucket_idx))
           / LAG(avg_close) OVER (PARTITION BY symbol ORDER BY bucket_idx)
    END AS change_ratio
  FROM avg_per_bucket
)
SELECT
  c.symbol,
  c.bucket_idx,
  c.bucket_start,
  c.avg_close,
  c.prev_avg_close,
  c.change_ratio
FROM change_vs_prev c
CROSS JOIN (SELECT CAST(${RATIO} AS DOUBLE) AS ratio_threshold) p
WHERE c.change_ratio IS NOT NULL
  AND ABS(c.change_ratio) >= p.ratio_threshold
;

-- View 2: industry grouping using the flagged view
CREATE OR REPLACE TEMP VIEW industry_grouped AS
WITH sym2ind AS (
  SELECT symbol, industry FROM tsdb_profiles
),
flag_with_industry AS (
  SELECT f.*, s.industry
  FROM flagged f
  JOIN sym2ind s USING (symbol)
),
industry_bucket_hit AS (
  SELECT DISTINCT industry, bucket_idx FROM flag_with_industry
),
change_vs_prev AS (
  -- recompute once here to keep the logic identical
  WITH params AS (
    SELECT
      CAST('${START_DATE}' AS TIMESTAMP)               AS start_ts,
      CAST(${WINDOW_DAYS} * 86400 AS BIGINT)          AS window_sec
  ),
  buckets AS (
    SELECT
      o.symbol, o.ts, o.close, p.start_ts, p.window_sec,
      CAST(unix_timestamp(o.ts) - unix_timestamp(p.start_ts) AS BIGINT) AS delta_sec
    FROM tsdb_stock_ohlc o CROSS JOIN params p
    WHERE o.ts >= p.start_ts
  ),
  bucketed AS (
    SELECT
      symbol,
      CAST(FLOOR(delta_sec / window_sec) AS BIGINT) AS bucket_idx,
      to_timestamp(unix_timestamp(start_ts) + (FLOOR(delta_sec / window_sec) * window_sec)) AS bucket_start,
      close
    FROM buckets
  ),
  avg_per_bucket AS (
    SELECT symbol, bucket_idx, bucket_start, AVG(close) AS avg_close
    FROM bucketed
    GROUP BY symbol, bucket_idx, bucket_start
  )
  SELECT
    symbol,
    bucket_idx,
    bucket_start,
    avg_close,
    LAG(avg_close) OVER (PARTITION BY symbol ORDER BY bucket_idx) AS prev_avg_close,
    CASE
      WHEN LAG(avg_close) OVER (PARTITION BY symbol ORDER BY bucket_idx) IS NULL THEN NULL
      WHEN LAG(avg_close) OVER (PARTITION BY symbol ORDER BY bucket_idx) = 0 THEN NULL
      ELSE (avg_close - LAG(avg_close) OVER (PARTITION BY symbol ORDER BY bucket_idx))
           / LAG(avg_close) OVER (PARTITION BY symbol ORDER BY bucket_idx)
    END AS change_ratio
  FROM avg_per_bucket
)
SELECT
  s.industry,
  f.bucket_idx,
  f.bucket_start,
  f.symbol,
  f.avg_close,
  f.prev_avg_close,
  f.change_ratio
FROM change_vs_prev f
JOIN sym2ind s USING (symbol)
JOIN industry_bucket_hit h
  ON h.industry = s.industry AND h.bucket_idx = f.bucket_idx
;

-- ---------- Sinks: define JDBC data-source tables (pointers to TSDB) ----------
CREATE TABLE IF NOT EXISTS tsdb_w1_symbol_changes
USING org.apache.spark.sql.jdbc
OPTIONS (
  url      "${JDBC_URL}",
  driver   "org.postgresql.Driver",
  dbtable  "${SCHEMA}.w1_symbol_changes",
  user     "${JDBC_USER}",
  password "${JDBC_PASSWORD}"
);

CREATE TABLE IF NOT EXISTS tsdb_w1_industry_windows
USING org.apache.spark.sql.jdbc
OPTIONS (
  url      "${JDBC_URL}",
  driver   "org.postgresql.Driver",
  dbtable  "${SCHEMA}.w1_industry_windows",
  user     "${JDBC_USER}",
  password "${JDBC_PASSWORD}"
);

-- ---------- Write results INTO TimescaleDB ----------
INSERT OVERWRITE TABLE tsdb_w1_symbol_changes
SELECT symbol, bucket_start, avg_close, prev_avg_close, change_ratio
FROM flagged;

INSERT OVERWRITE TABLE tsdb_w1_industry_windows
SELECT industry, bucket_start, symbol, avg_close, prev_avg_close, change_ratio
FROM industry_grouped;