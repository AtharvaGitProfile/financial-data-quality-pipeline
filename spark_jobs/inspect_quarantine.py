from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("inspect_quarantine").getOrCreate()

df = spark.read.parquet("/tmp/datatrust_quarantine/quarantine.parquet")

df.select(
    "error_reason",
    "raw_value",
    "event_id",
    "transaction_id",
    "user_id",
    "amount",
    "currency",
    "status",
    "event_time",
).show(50, truncate=120)

spark.stop()
