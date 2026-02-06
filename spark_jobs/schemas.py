from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType
)

fintech_transaction_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("merchant_id", StringType(), False),

    StructField("amount", StringType(), False),      # keep string (fintech best practice)
    StructField("currency", StringType(), False),

    StructField("payment_method", StringType(), True),
    StructField("transaction_type", StringType(), True),

    StructField("status", StringType(), False),
    StructField("failure_reason", StringType(), True),

    StructField("event_time", StringType(), False),
    StructField("ingested_at", StringType(), True),

    StructField("risk_score", IntegerType(), True),
    StructField("source_system", StringType(), True),
    StructField("schema_version", IntegerType(), True),
])
