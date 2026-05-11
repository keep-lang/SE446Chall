# debug_spark.py
# Bypass Streamlit and test whether Spark can read the JSON files
# and run the three aggregations against them in BATCH mode (not streaming).
# If batch reads work, the data is fine; the bug is in the streaming setup.
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = (SparkSession.builder
         .appName("debug").master("local[*]")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("source", StringType(),    True),
    StructField("title",  StringType(),    True),
    StructField("url",    StringType(),    True),
    StructField("ts",     TimestampType(), True),
])

print("=" * 60)
print("Reading data/incoming/*.json")
df = spark.read.schema(schema).json("data/incoming")
print(f"Total rows: {df.count()}")
print()
print("Sample 5 rows:")
df.show(5, truncate=80)
print()
print("by_source:")
df.groupBy("source").count().orderBy(F.col("count").desc()).show()
print()
print("top words:")
(df.select(F.explode(F.regexp_extract_all(F.lower(F.col("title")),
                                          F.lit(r"([a-z]{3,})"),
                                          F.lit(1))).alias("word"))
   .groupBy("word").count().orderBy(F.col("count").desc()).show(15))
print("=" * 60)
spark.stop()
