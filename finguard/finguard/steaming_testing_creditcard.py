# Databricks notebook source
BOOTSTRAP_SERVERS="pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092"
API_KEY="SZMBWX4CCVIVQKPK"
API_SECRET="cfltyrkM67GeOgkqApvLR0YlorgGTiHM7IanqvxXeXFwZWXjMWewLhO8jBKbteRw"
TOPIC_NAME="CREDIT_CARD_TRANSCATIONS"

# COMMAND ----------

jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="{API_KEY}" \
    password="{API_SECRET}";'

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

spark_streamdata=spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
    .option("subscribe", TOPIC_NAME)\
    .option("kafka.security.protocol", "SASL_SSL")\
    .option("kafka.sasl.jaas.config", jaas_config)\
    .option("kafka.sasl.mechanism", "PLAIN")\
    .option("startingOffsets","earliest")\
    .load()


# COMMAND ----------

parsed_streamdata=spark_streamdata.select(col("key").cast("string"), col("value").cast("string"),col("topic"),col("partition"),col("offset"),col("timestamp"),col("timestampType"))

# COMMAND ----------

spark_query_id=(parsed_streamdata.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "/Volumes/finguard/source/checkpoint")\
        .trigger(availableNow=True)\
            .toTable("finguard.bronze.stream_credit_testing"))

# COMMAND ----------

df=spark.sql("select * from finguard.bronze.stream_credit_testing")
df.schema