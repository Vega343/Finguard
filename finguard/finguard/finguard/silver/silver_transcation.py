from pyspark import pipelines as dp
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
import json
from pyspark.sql.dataframe import DataFrame

@dp.table(
    name="finguard.silver.transcational_stream",
    comment="This is the silver table for the transcational stream"
)
@dp.expect_or_drop("Transcation","transaction_id IS NOT NULL")
@dp.expect_or_drop("merchant_id","merchant_id IS NOT NULL")
@dp.expect_or_drop("merchant_category","merchant_category IS NOT NULL")
@dp.expect_or_drop("amount","amount IS NOT NULL")
@dp.expect_or_drop("currency","currency IS NOT NULL")
@dp.expect("amount valis","amount>0")
def silver_transctional():
    df_bronze=spark.readStream.table("finguard.bronze.transcational_stream")
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("card_number", StringType()),
        StructField("merchant_id", StringType()),
        StructField("merchant_name", StringType()),
        StructField("merchant_category", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("transaction_type", StringType()),
        StructField("payment_channel", StringType()),
        StructField("device_id", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("transaction_timestamp", StringType()),
        StructField("is_international", BooleanType()),
        StructField("status", StringType()),
    ])
    parsed_df = (
    df_bronze
    .select(
        F.from_json(F.col("value"), schema).alias("data"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestampType").alias("kafka_timestamp_type"),
        F.col("ingesttime_timestamp").alias("bronze_ingesttime_timestamp"),
    )
    .select(
        "data.*",
        "kafka_timestamp",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp_type",
        "bronze_ingesttime_timestamp",
        F.current_timestamp().alias("silver_ingesttime_timestamp"),
    )
    )
    return parsed_df
   