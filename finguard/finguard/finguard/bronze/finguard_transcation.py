from pyspark import pipelines as dp
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
import json
from pyspark.sql.dataframe import DataFrame

@dp.table(
    name="finguard.bronze.transcational_stream",
    comment="This is the bronze table for the transcational stream"
)
def transcational_stream():
    BOOTSTRAP_SERVERS=spark.conf.get("BOOTSTRAP_SERVERS")
    API_KEY=spark.conf.get("API_KEY")
    API_SECRET=spark.conf.get("API_SECRET")
    TOPIC_NAME=spark.conf.get("TOPIC_NAME")

    jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="{API_KEY}" \
    password="{API_SECRET}";'

    spark_streamdata=spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
    .option("subscribe", TOPIC_NAME)\
    .option("kafka.security.protocol", "SASL_SSL")\
    .option("kafka.sasl.jaas.config", jaas_config)\
    .option("kafka.sasl.mechanism", "PLAIN")\
    .option("startingOffsets","earliest")\
    .load()

    parsed_streamdata=spark_streamdata.select(col("key").cast("string"), col("value").cast("string"),col("topic"),col("partition"),col("offset"),col("timestamp"),col("timestampType"),current_timestamp().alias("ingesttime_timestamp"))

    return parsed_streamdata