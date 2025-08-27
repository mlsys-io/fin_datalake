CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.w1_symbol_changes (
  symbol TEXT NOT NULL,
  bucket_start TIMESTAMPTZ NOT NULL,
  avg_close DOUBLE PRECISION,
  prev_avg_close DOUBLE PRECISION,
  change_ratio DOUBLE PRECISION,
  PRIMARY KEY (symbol, bucket_start)
);
CREATE INDEX IF NOT EXISTS w1_symbol_changes_ts_idx
  ON public.w1_symbol_changes (bucket_start DESC);

CREATE TABLE IF NOT EXISTS public.w1_industry_windows (
  industry TEXT NOT NULL,
  bucket_start TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  avg_close DOUBLE PRECISION,
  prev_avg_close DOUBLE PRECISION,
  change_ratio DOUBLE PRECISION,
  PRIMARY KEY (industry, bucket_start, symbol)
);
CREATE INDEX IF NOT EXISTS w1_industry_windows_ts_idx
  ON public.w1_industry_windows (bucket_start DESC, industry);
