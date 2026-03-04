from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark
spark = SparkSession.builder \
    .appName("CDC_Cards_Bronze") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .enableHiveSupport() \
    .getOrCreate()

# Configuration
KAFKA_SERVERS = "localhost:9092"
TOPIC = "finance.public.cards"
OUTPUT_PATH = "/user/hive/warehouse/dl_bronze.db/cards"
CHECKPOINT_PATH = f"{OUTPUT_PATH}/_checkpoints"


# Set table/database name
TABLE_NAME = "cards"
DATABASE_NAME = "lake_bronze"

# Define schema for cards, matching Postgres DDL
card_after_schema = StructType([
    StructField("card_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("card_num", StringType()),
    StructField("card_brand", StringType()),
    StructField("card_type", StringType()),
    StructField("credit_limit", FloatType()),
    StructField("exp_date", StringType()),
    StructField("cvv", StringType()),
    StructField("cardholder_name", StringType()),
    StructField("is_active", BooleanType())
])

card_envelope_schema = StructType([
    StructField("after", card_after_schema)
])

# Ensure database exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

# Create table if not exists (no partition)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{TABLE_NAME} (
        card_id STRING,
        customer_id STRING,
        card_num STRING,
        card_brand STRING,
        card_type STRING,
        credit_limit DOUBLE,
        exp_date STRING,
        cvv STRING,
        cardholder_name STRING,
        is_active BOOLEAN,
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
cards = kafka_stream.selectExpr("CAST(value AS STRING) AS json") \
    .select(from_json(col("json"), card_envelope_schema).alias("data")) \
    .select("data.after.*") \
    .where("card_id IS NOT NULL") \
    .withColumn("load_at", current_timestamp())

# Write to HDFS (no partition)
query = cards.coalesce(2) \
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