from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, when, to_date

SILVER_PATH = "s3a://datatrust-lake/silver/fintech_transactions"
GOLD_PATH   = "s3a://datatrust-lake/gold/daily_transaction_metrics"

def main():
    spark = (
        SparkSession.builder
        .appName("silver_to_gold_metrics")
        .getOrCreate()
    )

    silver = spark.read.parquet(SILVER_PATH)

    gold = (
        silver
        .withColumn("event_date", to_date(col("event_time")))
        .groupBy("event_date", "currency", "status")
        .agg(
            count("*").alias("transaction_count"),
            _sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            _sum(
                when(col("risk_score") >= 80, 1).otherwise(0)
            ).alias("high_risk_txn_count")
        )
    )

    gold.write.mode("overwrite").parquet(GOLD_PATH)

    spark.stop()

if __name__ == "__main__":
    main()
