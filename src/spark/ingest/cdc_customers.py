from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark
spark = SparkSession.builder \
    .appName("CDC_users_Bronze") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .enableHiveSupport() \
    .getOrCreate()

# Configuration
KAFKA_SERVERS = "localhost:9092"
TOPIC = "finance.public.users"
OUTPUT_PATH = "/user/hive/warehouse/dl_bronze.db/users"
CHECKPOINT_PATH = f"{OUTPUT_PATH}/_checkpoints"


# Set table/database name
TABLE_NAME = "users"
DATABASE_NAME = "lake_bronze"

# Define schema for users, matching Postgres customers table
user_after_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("ssn", StringType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("gender", StringType()),
    StructField("street", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("zip", StringType()),
    StructField("lat", FloatType()),
    StructField("long", FloatType()),
    StructField("city_pop", IntegerType()),
    StructField("job", StringType()),
    StructField("dob", StringType()),
    StructField("acct_num", StringType()),
    StructField("profile", StringType())
])

user_envelope_schema = StructType([
    StructField("after", user_after_schema)
])

# Ensure database exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

# Create table if not exists (no partition)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{TABLE_NAME} (
        customer_id STRING,
        ssn STRING,
        first_name STRING,
        last_name STRING,
        gender STRING,
        street STRING,
        city STRING,
        state STRING,
        zip STRING,
        lat DOUBLE,
        long DOUBLE,
        city_pop INT,
        job STRING,
        dob STRING,
        acct_num STRING,
        profile STRING,
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
users = kafka_stream.selectExpr("CAST(value AS STRING) AS json") \
    .select(from_json(col("json"), user_envelope_schema).alias("data")) \
    .select("data.after.*") \
    .where("customer_id IS NOT NULL") \
    .withColumn("load_at", current_timestamp())

# Write to HDFS (no partition)
query = users.coalesce(2) \
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