# streaming_job.py
# T2 - Spark Structured Streaming job with three memory sinks: by_source, by_window, top_words
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = (
    SparkSession.builder
        .appName("NewsPulse")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Schema for the JSONL files produced by ingester.py
schema = StructType([
    StructField("source", StringType(),    True),
    StructField("title",  StringType(),    True),
    StructField("url",    StringType(),    True),
    StructField("ts",     TimestampType(), True),
])

# file-source streaming read: "Spark watches a folder; new file = new mini-batch"
stream = (
    spark.readStream
        .schema(schema)
        .option("maxFilesPerTrigger", 1)
        .json("data/incoming")
)

# -----------------------------------------------------------------------------
# (1) by_source  - groupBy("source").count(), output mode complete
# -----------------------------------------------------------------------------
q_src = (
    stream.groupBy("source").count()
        .writeStream
        .outputMode("complete")
        .format("memory")
        .queryName("by_source")
        .start()
)

# -----------------------------------------------------------------------------
# (2) by_window  - 1-hour tumbling window with 2-hour watermark to bound state
# -----------------------------------------------------------------------------
windowed = (
    stream
        .withWatermark("ts", "2 minutes")
        .groupBy(F.window("ts", "1 minute").alias("w"))
        .count()
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("count"),
        )
)

q_win = (
    windowed.writeStream
        .outputMode("append")
        .format("memory")
        .queryName("by_window")
        .start()
)

# -----------------------------------------------------------------------------
# (3) top_words  - tokenise title, drop stop-words and short tokens, count
# -----------------------------------------------------------------------------
STOP_WORDS = {
    "the","a","an","and","or","but","if","then","else","for","of","to","in","on",
    "at","by","with","from","is","are","was","were","be","been","being","this",
    "that","these","those","it","its","as","not","no","do","does","did","has",
    "have","had","will","would","can","could","should","may","might","says","say",
    "said","new","over","into","up","down","out","off","about","after","before",
    "you","your","we","our","i","he","she","they","them","his","her","their",
    "us","more","than","also","just","like","get","got","one","two","amid","via",
}
stop_bcast = spark.sparkContext.broadcast(STOP_WORDS)

words = (
    stream
        .select(F.explode(F.regexp_extract_all(F.lower(F.col("title")),
                                               F.lit(r"([a-z]{3,})"),
                                               F.lit(1))).alias("word"))
)
# filter stop-words inside a UDF-free path: use isin against a literal array
stop_list = sorted(STOP_WORDS)
words = words.where(~F.col("word").isin(stop_list))

q_words = (
    words.groupBy("word").count()
        .writeStream
        .outputMode("complete")
        .format("memory")
        .queryName("top_words")
        .start()
)

print("[streaming_job] all three queries started. Sinks: by_source, by_window, top_words")
print("[streaming_job] sanity-check with:  spark.sql('select * from by_source').show()")

spark.streams.awaitAnyTermination()
