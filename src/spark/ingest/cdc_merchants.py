from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark
spark = SparkSession.builder \
    .appName("CDC_Merchants_Bronze") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .enableHiveSupport() \
    .getOrCreate()

# Configuration
KAFKA_SERVERS = "localhost:9092"
TOPIC = "finance.public.merchants"
OUTPUT_PATH = "/user/hive/warehouse/dl_bronze.db/merchants"
CHECKPOINT_PATH = f"{OUTPUT_PATH}/_checkpoints"

# Set table/database name
TABLE_NAME = "merchants"
DATABASE_NAME = "lake_bronze"

# Define schema for merchants, matching Postgres DDL
merchant_after_schema = StructType([
    StructField("merchant_id", StringType()),
    StructField("merchant_name", StringType()),
    StructField("category", StringType())
])

merchant_envelope_schema = StructType([
    StructField("after", merchant_after_schema)
])

# Ensure database exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

# Create table if not exists (no partition)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{TABLE_NAME} (
        merchant_id STRING,
        merchant_name STRING,
        category STRING,
        load_at TIMESTAMP
    )
    USING PARQUET
    LOCATION '{OUTPUT_PATH}'
""")

# Process stream
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse & transform
merchants = kafka_stream.selectExpr("CAST(value AS STRING) AS json") \
    .select(from_json(col("json"), merchant_envelope_schema).alias("data")) \
    .select("data.after.*") \
    .where("merchant_id IS NOT NULL") \
    .withColumn("load_at", current_timestamp())

# Write to HDFS (no partition)
query = merchants.coalesce(2) \
    .writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="30 seconds") \
    .start(OUTPUT_PATH)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
    spark.stop()
