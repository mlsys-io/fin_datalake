# local:///opt/app/delta2timescale.py
import os
from pyspark.sql import SparkSession, functions as F

# ---- Inputs via env (simple & k8s-friendly) ----
# Read from either a Hive/Delta table name or a Delta path
DELTA_TABLE = os.getenv("DELTA_TABLE")          # e.g. "default.my_delta_table"
DELTA_PATH  = os.getenv("DELTA_PATH")           # e.g. "s3a://bucket/path/to/delta"

# Timescale/Postgres connection (standalone service)
PG_HOST     = os.getenv("PG_HOST", "timescaledb.default.svc.cluster.local")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DB       = os.getenv("PG_DB", "postgres")
PG_SCHEMA   = os.getenv("PG_SCHEMA", "public")
PG_TABLE    = os.getenv("PG_TABLE", "processed_reports")
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")
PG_SSLMODE  = os.getenv("PG_SSLMODE", "disable")  # change to 'require' if needed

jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?sslmode={PG_SSLMODE}"
target_table = f"{PG_SCHEMA}.{PG_TABLE}"

spark = (
    SparkSession.builder.appName("delta2timescale")
    # Delta & JDBC extensions are provided by the image / sparkConf / packages
    .getOrCreate()
)

# ---- Read the Delta table ----
if DELTA_TABLE:
    df = spark.table(DELTA_TABLE)
elif DELTA_PATH:
    df = spark.read.format("delta").load(DELTA_PATH)
else:
    raise ValueError("Set either DELTA_TABLE or DELTA_PATH")

# ---- Transform: add processed_report (underscore to avoid quoted identifiers in Postgres) ----
# If you truly want a space: use `"processed report"` below instead of "processed_report".
df_out = (
    df
    .withColumn(
        "processed_report",
        F.concat(
            F.lit("According to "),
            F.coalesce(F.col("source"), F.lit("unknown source")),
            F.lit(", "),
            F.coalesce(F.col("summary"), F.lit("")),
        )
    )
    .withColumn("ingested_at", F.current_timestamp())  # handy if you later convert to a hypertable
    .select("source", "summary", "processed_report", "ingested_at")
)

# ---- Write to Timescale/Postgres via JDBC ----
(
    df_out.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", target_table)
    .option("user", PG_USER)
    .option("password", PG_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .option("batchsize", "5000")     # tune as needed
    .mode("append")
    .save()
)

spark.stop()
