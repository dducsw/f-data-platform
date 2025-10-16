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

# Create Bronze database and table
spark.sql("CREATE DATABASE IF NOT EXISTS dl_bronze")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS dl_bronze.users (
        -- Kafka metadata
        kafka_key STRING,
        kafka_offset BIGINT,
        kafka_timestamp TIMESTAMP,
        
        -- Raw CDC data
        cdc_payload STRING,
        
        -- Basic CDC info
        operation STRING,
        cdc_timestamp BIGINT,
        
        -- Bronze metadata
        ingestion_time TIMESTAMP,
        ingestion_date STRING
    )
    USING PARQUET
    PARTITIONED BY (ingestion_date)
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

# Transform to Bronze format
bronze_df = kafka_stream.select(
    # Kafka info
    col("key").cast("string").alias("kafka_key"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp"),
    
    # Raw CDC 
    col("value").cast("string").alias("cdc_payload"),
    
    # Parse for op and ts
    get_json_object(col("value").cast("string"), "$.op").alias("operation"),
    get_json_object(col("value").cast("string"), "$.ts_ms").cast("bigint").alias("cdc_timestamp"),
    
    # Bronze metadata
    current_timestamp().alias("ingestion_time"),
    date_format(current_timestamp(), "yyyy-MM-dd").alias("ingestion_date")
)

# Simple write function
def write_batch(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write \
            .mode("append") \
            .insertInto("bronze.users")
        
        print(f"Batch {batch_id}: {batch_df.count()} records written to Bronze")

# Start streaming
query = bronze_df \
    .writeStream \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="30 seconds") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
    spark.stop()