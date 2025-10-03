from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, year, month, dayofmonth, hour, regexp_replace,
    lit, current_timestamp, sum, count
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
from datetime import datetime
import sys

print("Transform transactions to silver layer")

# ---------------------------
# Create Spark session
spark = (SparkSession.builder
         .appName('Transform table "transactions" from bronze to silver layer')
         .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
         .config("spark.sql.catalogImplementation", "hive")
         .config("hive.metastore.uris", "thrift://localhost:9083")
         .config("spark.driver.memory", "8g")  
         .config("spark.executor.memory", "8g") 
         .config("spark.executor.cores", "6")
         .config("spark.driver.maxResultSize", "4g")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ---------------------------
# Create Silver database
spark.sql("CREATE DATABASE IF NOT EXISTS dlh_silver")

# ---------------------------
# Create Silver table if not exists
create_table_sql = """
CREATE TABLE IF NOT EXISTS dlh_silver.transactions (
    transaction_id      INT,
    client_id           INT,
    client_key          INT,
    card_id             INT,
    card_key            INT,
    date_time           TIMESTAMP,
    day                 INT,
    hour                INT,
    amount              DECIMAL(18,4),
    use_chip            BOOLEAN,
    merchant_id         INT,
    merchant_city       STRING,
    merchant_state      STRING,
    zip                 STRING,
    mcc                 INT,
    errors              STRING,
    updated_at          TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);
"""
spark.sql(create_table_sql)

# ---------------------------
try:
    print('Start transform table "transactions"')
    batch_start_time = datetime.now()

    # Read bronze and silver tables
    df_bronze = spark.table("dlh_bronze.transactions").filter(year(col("date_time")) >= 2019)
    df_users = spark.table("dlh_silver.users").filter(col("is_current") == True)
    df_cards = spark.table("dlh_silver.cards").filter(col("is_current") == True)

    print(f"Bronze records count: {df_bronze.count()}")
    print(f"Users records count: {df_users.count()}")
    print(f"Cards records count: {df_cards.count()}")
        
    # Transform batch
    df_transformed = (df_bronze
            .withColumn("transaction_id", col("id"))
            .withColumn("amount_clean", regexp_replace(col("amount"), "[$,]", "").cast(DecimalType(18,4)))
            .withColumn("year", year(col("date_time")))
            .withColumn("month", month(col("date_time")))
            .withColumn("day", dayofmonth(col("date_time")))
            .withColumn("hour", hour(col("date_time")))
            .withColumn("use_chip", when(col("use_chip").isin(["Yes", "yes"]), True).otherwise(False))
            .withColumn("updated_at", current_timestamp())
            .withColumn("event_ts", col("date_time").cast("long"))
        )

    # Join with users and cards
    df_with_users = df_transformed.join(
            df_users.select(col("client_id"), col("client_key")),
            on="client_id",
            how="left"
        )

    df_with_cards = df_with_users.join(
            df_cards.select(col("card_id"), col("card_key")),
            on="card_id",
            how="left"
        )


    # Select final schema
    df_silver_batch = df_with_cards.select(
            col("transaction_id"),
            col("client_id"),
            col("client_key"),
            col("card_id"),
            col("card_key"),
            col("date_time"),
            col("year"),
            col("month"),
            col("day"),
            col("hour"),
            col("amount_clean").alias("amount"),
            col("use_chip"),
            col("merchant_id"),
            col("merchant_city"),
            col("merchant_state"),
            col("zip"),
            col("mcc"),
            col("errors"),
            col("updated_at")
        )

    # Write batch to Silver Layer
    df_silver_batch.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .saveAsTable("dlh_silver.transactions")

    batch_end_time = datetime.now()
    processing_time = (batch_end_time - batch_start_time).total_seconds()

    print(f"Transform completed successfully in {processing_time:.2f} seconds")
    # print(f"Records processed: {df_silver.count()}")

except Exception as e:
    print(f"-- Error: {e}")
    sys.exit(1)
