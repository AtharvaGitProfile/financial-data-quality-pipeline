from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, when, to_date

SILVER_PATH = "s3a://datatrust-lake/silver/fintech_transactions"
GOLD_PATH   = "s3a://datatrust-lake/gold/daily_transaction_metrics"

S3_ENDPOINT = "http://localstack:4566"

def main():
    spark = (
        SparkSession.builder
        .appName("silver_to_gold_metrics")
        # LocalStack S3A config (same idea as earlier)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.region", "us-east-1")
        .getOrCreate()
    )

    silver = spark.read.parquet(SILVER_PATH)

    # amount is a string in Silver; cast to double for math
    silver_typed = silver.withColumn("amount_num", col("amount").cast("double"))

    gold = (
        silver_typed
        .withColumn("event_date", to_date(col("event_time")))
        .groupBy("event_date", "currency", "status")
        .agg(
            count("*").alias("transaction_count"),
            _sum("amount_num").alias("total_amount"),
            avg("amount_num").alias("avg_amount"),
            _sum(when(col("risk_score") >= 80, 1).otherwise(0)).alias("high_risk_txn_count")
        )
    )

    gold.write.mode("overwrite").parquet(GOLD_PATH)

    spark.stop()

if __name__ == "__main__":
    main()
