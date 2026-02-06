from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, current_timestamp
from pyspark.sql.types import StringType

from schemas import fintech_transaction_schema

S3_BUCKET = "datatrust-lake"
BRONZE_PATH = f"s3a://{S3_BUCKET}/bronze/fintech_transactions"
SILVER_PATH = f"s3a://{S3_BUCKET}/silver/fintech_transactions"
QUAR_PATH   = f"s3a://{S3_BUCKET}/quarantine/fintech_transactions"

S3_ENDPOINT = "http://localstack:4566"


def build_spark():
    return (
        SparkSession.builder
        .appName("silver_bronze_to_silver")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # 1) Read Bronze (raw inbox)
    bronze = spark.read.parquet(BRONZE_PATH)

    # 2) Parse JSON into a structured column using the schema
    parsed = bronze.withColumn(
        "parsed",
        from_json(col("raw_value"), fintech_transaction_schema)
    )

    # 3) Pull fields out of parsed struct
    flattened = parsed.select(
        col("raw_value"),
        col("topic"), col("partition"), col("offset"),
        col("kafka_timestamp"),
        col("ingested_at").alias("bronze_ingested_at"),
        col("parsed.*")
    )

    # 4) Define "bad record" rules (required fields missing)
    required_missing = (
        col("event_id").isNull()
        | col("transaction_id").isNull()
        | col("user_id").isNull()
        | col("merchant_id").isNull()
        | col("amount").isNull()
        | col("currency").isNull()
        | col("status").isNull()
        | col("event_time").isNull()
    )

    good = flattened.filter(~required_missing).withColumn("silver_loaded_at", current_timestamp())

    bad = (
        flattened.filter(required_missing)
        .withColumn("error_reason", lit("SCHEMA_OR_REQUIRED_FIELD_FAILURE").cast(StringType()))
        .withColumn("quarantined_at", current_timestamp())
    )

    # 5) Write outputs
    good.write.mode("append").parquet(SILVER_PATH)
    bad.write.mode("append").parquet(QUAR_PATH)


if __name__ == "__main__":
    main()
