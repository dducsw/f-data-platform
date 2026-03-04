from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark
spark = SparkSession.builder \
    .appName("CDC_transactions_Bronze") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .enableHiveSupport() \
    .getOrCreate()

# Configuration
KAFKA_SERVERS = "localhost:9092"
TOPIC = "finance.public.transactions"
OUTPUT_PATH = "/user/hive/warehouse/dl_bronze.db/transactions"
CHECKPOINT_PATH = f"{OUTPUT_PATH}/_checkpoints"
TABLE_NAME = "transaction"
DATABASE_NAME = "lake_bronze"

# Define schema for transaction, matching Postgres DDL
transaction_after_schema = StructType([
    StructField("trans_num", StringType()),
    StructField("trans_date", StringType()),
    StructField("trans_time", StringType()),
    StructField("unix_time", LongType()),
    StructField("category", StringType()),
    StructField("amt", DoubleType()),
    StructField("is_fraud", IntegerType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_name", StringType()),
    StructField("merch_lat", DoubleType()),
    StructField("merch_long", DoubleType()),
    StructField("customer_id", StringType()),
    StructField("card_id", StringType()),
    StructField("trans_type", StringType())
])

transaction_envelope_schema = StructType([
    StructField("after", transaction_after_schema)
])

# Process stream
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()


# Parse & transform
transaction = kafka_stream.selectExpr("CAST(value AS STRING) AS json") \
    .select(from_json(col("json"), transaction_envelope_schema).alias("data")) \
    .select("data.after.*") \
    .withColumn("txn_num", col("trans_num")) \
    .withColumn("txn_date", col("trans_date")) \
    .withColumn("txn_time", col("trans_time")) \
    .withColumn("txn_category", col("category")) \
    .withColumn("txn_amount", col("amt")) \
    .withColumn("txn_type", col("trans_type")) \
    .where("trans_num IS NOT NULL") \
    .withColumn("partition", date_format(to_date(col("trans_date"), "yyyy-MM-dd"), "yyyyMMdd")) \
    .withColumn("load_at", current_timestamp()) \
    .select(
        "txn_num", "txn_date", "txn_time", "unix_time",
        "txn_category", "txn_amount", "is_fraud",
        "merchant_id", "merchant_name", "merch_lat", "merch_long",
        "customer_id", "card_id", "txn_type",
        "partition", "load_at"
    )

# Ensure database exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")


# Create table if not exists, matching Postgres DDL
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{TABLE_NAME} (
        txn_num STRING,
        txn_date STRING,
        txn_time STRING,
        unix_time BIGINT,
        txn_category STRING,
        txn_amount DOUBLE,
        is_fraud INT,
        merchant_id STRING,
        merchant_name STRING,
        merch_lat DOUBLE,
        merch_long DOUBLE,
        customer_id STRING,
        card_id STRING,
        txn_type STRING,
        partition STRING,
        load_at TIMESTAMP
    )
    USING PARQUET
    PARTITIONED BY (partition)
    LOCATION '{OUTPUT_PATH}'
""")


# Write to HDFS
query = transaction.coalesce(2) \
    .writeStream \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="30 seconds") \
    .start(OUTPUT_PATH)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
    spark.stop()