# T5 Reflection

At 1000x input the first thing to break is the ingester writing one JSONL
file per tick to a single local folder: Spark's file source has to list
`data/incoming/` every micro-batch, and small-file overhead plus filesystem
listing dominates. The `complete`-mode `top_words` query is the second
bottleneck because its in-memory state grows with the vocabulary and the
shuffle has only four partitions. I would reach for Structured Streaming
on Kafka (one topic, partitioned by source) as the ingest path, switch
`top_words` to an `append`-mode windowed aggregation with a watermark to
bound state, and raise `spark.sql.shuffle.partitions` to match the cores.
