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
         .config("spark.driver.memory", "4g")  
         .config("spark.executor.memory", "4g") 
         .config("spark.executor.cores", "2")
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
-- Bảng transaction_detail
CREATE TABLE IF NOT EXISTS dlh_gold.transaction_detail (
    -- Transaction identifiers
    transaction_id              BIGINT,

    -- Client info
    client_key                  BIGINT,
    client_id                   INT,
    client_age                  INT,
    client_latitude             DECIMAL(9,6),
    client_longitude            DECIMAL(9,6),
    client_credit_score         INT,
    client_num_credit_cards     INT,

    -- Card info
    card_key                    BIGINT,
    card_id                     INT,
    card_brand                  STRING,
    card_type                   STRING,
    card_open_year              INT,
    card_expires_year           INT,
    card_credit_limit           DECIMAL(12,2),
    card_on_dark_web            BOOLEAN,

    -- Transaction metrics
    amount                      DECIMAL(18,4),
    amount_ratio_by_date        DECIMAL(10,4),
    spending_ratio              DECIMAL(10,4),
    use_chip                    BOOLEAN,

    -- Date/Time
    date_time                   TIMESTAMP,
    year                        INT,
    month                       INT,
    day                         INT,
    hour                        INT,

    -- Merchant info
    merchant_id                 INT,
    merchant_city               STRING,
    merchant_state              STRING,
    merchant_zip                STRING,
    mcc                         INT,
    mcc_description             STRING,

    -- Flags
    is_error                    BOOLEAN,
    is_fraud                    BOOLEAN,

    -- Metadata
    created_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITIONED BY (year INT, month INT, day INT)
CLUSTERED BY (client_id) INTO 16 BUCKETS
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY',
    'parquet.enable.dictionary'='true',
);
"""
spark.sql(create_table_sql)

# ---------------------------
try:
    print('Start transform table "transactions"')
    batch_start_time = datetime.now()

    #Read silver tables
    df_transactions = spark.table("dlh_silver.transactions").filter(year(col("date_time")) >= 2019)
    df_users = spark.table("dlh_silver.users").filter(col("is_current") == True)
    df_cards = spark.table("dlh_silver.cards").filter(col("is_current") == True)

    print(f"Bronze records count: {df_transactions.count()}")
    print(f"Users records count: {df_users.count()}")
    print(f"Cards records count: {df_cards.count()}")
        
    # Transform batch

    # Join with users and cards
    df_with_users = df_transactions.join(
            df_users.select(col("client_id"), col("client_key"), col("birth_year"), col("latitude"), col("longtitude"),
                            col("credit_score"), col("num_credit_cards")
            on="client_id",
            how="left"
        )

    df_with_cards = df_with_users.join(
            df_cards.select(col("card_id"), col("card_key"), col("card_brand"), card("card_type"),
                            , col("acct_open_year"), col("expires_year"), col("credit_limit"), col("card_on_dark_web")
            on="card_id",
            how="left"
        )

    df_

    # Select final schema
    df_obt = df_with_velocity.select(
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
            col("velocity_1h"),
            col("velocity_24h"),
            col("amount_velocity_1h"),
            col("amount_velocity_24h"),
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
            .mode("append") \
            .partitionBy("year", "month") \
            .saveAsTable("dlh_silver.transactions")

    batch_end_time = datetime.now()
    processing_time = (batch_end_time - batch_start_time).total_seconds()

    print(f"Transform completed successfully in {processing_time:.2f} seconds")
    # print(f"Records processed: {df_silver.count()}")

except Exception as e:
    print(f"-- Error: {e}")
    sys.exit(1)
