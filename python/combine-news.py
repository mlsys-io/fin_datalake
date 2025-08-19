from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import collect_list, concat_ws

spark = SparkSession.builder.appName("combine-news").getOrCreate()

# --- read
df = spark.read.option("multiLine", "true").option("recursiveFileLookup", "true").json("s3a://raw-dataset/News/")
print("Size of DB:", df.count())

# 2) Collapse every "summary" into one long string (proper aggregation).
if df.rdd.isEmpty():
    out = spark.createDataFrame([("",)], ["combined_summary"])
else:
    out = df.select("summary") \
            .where(F.col("summary").isNotNull()) \
            .agg(F.concat_ws(" ", F.collect_list("summary")).alias("combined_summary"))

print("Size of out:", out.count())

# 3) Write exactly one part file into a directory (Spark always writes a folder).
(out.coalesce(1)
   .write.mode("overwrite")
   .json("s3a://raw-dataset/News-combined"))

spark.stop()