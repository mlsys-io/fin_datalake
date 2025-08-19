from pyspark.sql import SparkSession
import os

spark = (SparkSession.builder
         .appName("json-to-delta")
         .getOrCreate())

src_path  = os.getenv("SRC_JSON", "s3a://raw-dataset/News/")
dest_path = os.getenv("DEST_DELTA", "s3a://delta-lake/news")
table     = os.getenv("DELTA_TABLE", "news_delta")
mode      = os.getenv("WRITE_MODE", "overwrite") 

df = spark.read.option("multiLine", "true").option("recursiveFileLookup", "true").json(src_path)
print("Size of DB:", df.count())

# Single step: writes Delta files to dest_path AND registers the table in the metastore
(df.write.format("delta")
   .mode(mode)
   .option("path", dest_path)      # keeps data in S3 at dest_path (external table)
   .saveAsTable(table))

spark.stop()
