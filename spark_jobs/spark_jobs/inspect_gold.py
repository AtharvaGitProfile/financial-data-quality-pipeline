from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("inspect_gold").getOrCreate()

df = spark.read.parquet("s3a://datatrust-lake/gold/daily_transaction_metrics")
df.orderBy("event_date", "currency", "status").show(50, truncate=False)

spark.stop()
