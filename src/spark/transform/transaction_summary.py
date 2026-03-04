from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, min, max, countDistinct,
    when, round, current_timestamp, lit, desc, row_number, to_date
)
from pyspark.sql.window import Window
from datetime import datetime
import sys

print("Create daily transaction summary aggregation in gold layer")

spark = (SparkSession.builder
         .appName('Create transaction_summary - daily transaction metrics')
         .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
         .config("spark.sql.hive.metastore.version", "4.0.1")
         .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*")
         .config("spark.sql.catalogImplementation", "hive")
         .config("hive.metastore.uris", "thrift://localhost:9083")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

spark.sql("CREATE DATABASE IF NOT EXISTS dlh_gold")

create_table_sql = """
CREATE TABLE IF NOT EXISTS dlh_gold.transaction_summary (
    -- Date dimension
    txn_date                    STRING,
    
    -- Transaction Count Metrics
    txn_cnt                     BIGINT,
    txn_cnt_fraud               BIGINT,
    txn_cnt_legit               BIGINT,
    
    -- Financial Metrics
    txn_total                   DOUBLE,
    txn_total_fraud             DOUBLE,
    txn_total_legit             DOUBLE,
    txn_avg                     DOUBLE,
    txn_min                     DOUBLE,
    txn_max                     DOUBLE,
    
    -- Fraud & Risk Metrics
    fraud_rate                  DOUBLE,
    fraud_amount_rate           DOUBLE,
    
    -- Unique Counts
    unique_customers            BIGINT,
    unique_merchants            BIGINT,
    unique_cards                BIGINT,
    unique_categories           BIGINT,
    
    -- Top Performers
    top_category                STRING,
    top_category_cnt            BIGINT,
    top_merchant_id             STRING,
    top_merchant_name           STRING,
    top_merchant_txn_cnt        BIGINT,
    top_city                    STRING,
    top_city_revenue            DOUBLE,
    
    -- Time-based Metrics
    peak_hour                   INT,
    peak_hour_txn_cnt           BIGINT,
    
    -- Metadata
    created_at                  TIMESTAMP
)
USING PARQUET
PARTITIONED BY (txn_date)
OPTIONS ('compression'='snappy')
"""
spark.sql(create_table_sql)

try:
    batch_start_time = datetime.now()
    print("Start creating daily transaction summary")

    # Read silver transactions
    print("Reading silver layer tables...")
    df_transactions = spark.table("dlh_silver.transactions")
    df_customers = spark.table("dlh_silver.customers").filter(col("is_current") == True)
    df_merchants = spark.table("dlh_silver.merchants").filter(col("is_current") == True)

    # Aggregate transaction metrics by date
    print("Aggregating transaction metrics by date...")
    df_daily_stats = df_transactions.groupBy("txn_date").agg(
        # Transaction counts
        count("*").alias("txn_cnt"),
        sum(when(col("is_fraud") == 1, 1).otherwise(0)).alias("txn_cnt_fraud"),
        sum(when(col("is_fraud") == 0, 1).otherwise(0)).alias("txn_cnt_legit"),
        
        # Transaction amounts
        sum("txn_amount").alias("txn_total"),
        sum(when(col("is_fraud") == 1, col("txn_amount")).otherwise(0)).alias("txn_total_fraud"),
        sum(when(col("is_fraud") == 0, col("txn_amount")).otherwise(0)).alias("txn_total_legit"),
        avg("txn_amount").alias("txn_avg"),
        min("txn_amount").alias("txn_min"),
        max("txn_amount").alias("txn_max"),
        
        # Unique counts
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct("merchant_id").alias("unique_merchants"),
        countDistinct("card_id").alias("unique_cards"),
        countDistinct("txn_category").alias("unique_categories")
    )

    # Calculate fraud rates
    df_daily_stats = df_daily_stats \
        .withColumn("fraud_rate",
            round(
                when(col("txn_cnt") > 0, col("txn_cnt_fraud") / col("txn_cnt") * 100)
                .otherwise(0),
                2
            )
        ) \
        .withColumn("fraud_amount_rate",
            round(
                when(col("txn_total") > 0, col("txn_total_fraud") / col("txn_total") * 100)
                .otherwise(0),
                2
            )
        )

    # Find top category per day
    print("Finding top categories per day...")
    
    category_window = Window.partitionBy("txn_date").orderBy(desc("category_cnt"))
    
    df_top_category = df_transactions \
        .groupBy("txn_date", "txn_category") \
        .agg(count("*").alias("category_cnt")) \
        .withColumn("rank", row_number().over(category_window)) \
        .filter(col("rank") == 1) \
        .select(
            col("txn_date"),
            col("txn_category").alias("top_category"),
            col("category_cnt").alias("top_category_cnt")
        )

    # Find top merchant per day by transaction count
    print("Finding top merchants per day...")
    
    merchant_window = Window.partitionBy("txn_date").orderBy(desc("merchant_txn_cnt"))
    
    df_top_merchant = df_transactions \
        .groupBy("txn_date", "merchant_id", "merchant_name") \
        .agg(count("*").alias("merchant_txn_cnt")) \
        .withColumn("rank", row_number().over(merchant_window)) \
        .filter(col("rank") == 1) \
        .select(
            col("txn_date"),
            col("merchant_id").alias("top_merchant_id"),
            col("merchant_name").alias("top_merchant_name"),
            col("merchant_txn_cnt").alias("top_merchant_txn_cnt")
        )

    # Find top city by revenue per day
    print("Finding top cities per day...")
    
    city_window = Window.partitionBy("txn_date").orderBy(desc("city_revenue"))
    
    df_top_city = df_transactions \
        .join(
            df_customers.select(
                col("customer_id"),
                col("city")
            ),
            on="customer_id",
            how="left"
        ) \
        .groupBy("txn_date", "city") \
        .agg(sum("txn_amount").alias("city_revenue")) \
        .withColumn("rank", row_number().over(city_window)) \
        .filter(col("rank") == 1) \
        .select(
            col("txn_date"),
            col("city").alias("top_city"),
            round(col("city_revenue"), 2).alias("top_city_revenue")
        )

    # Find peak hour per day
    print("Finding peak hours per day...")
    
    hour_window = Window.partitionBy("txn_date").orderBy(desc("hour_txn_cnt"))
    
    df_peak_hour = df_transactions \
        .groupBy("txn_date", "txn_hour") \
        .agg(count("*").alias("hour_txn_cnt")) \
        .withColumn("rank", row_number().over(hour_window)) \
        .filter(col("rank") == 1) \
        .select(
            col("txn_date"),
            col("txn_hour").alias("peak_hour"),
            col("hour_txn_cnt").alias("peak_hour_txn_cnt")
        )

    # Join all insights
    print("Joining all daily insights...")
    df_summary = df_daily_stats \
        .join(df_top_category, on="txn_date", how="left") \
        .join(df_top_merchant, on="txn_date", how="left") \
        .join(df_top_city, on="txn_date", how="left") \
        .join(df_peak_hour, on="txn_date", how="left")

    # Final select
    df_final = df_summary \
        .withColumn("created_at", current_timestamp()) \
        .select(
            col("txn_date"),
            
            # Transaction Count Metrics
            col("txn_cnt"),
            col("txn_cnt_fraud"),
            col("txn_cnt_legit"),
            
            # Financial Metrics
            round(col("txn_total"), 2).alias("txn_total"),
            round(col("txn_total_fraud"), 2).alias("txn_total_fraud"),
            round(col("txn_total_legit"), 2).alias("txn_total_legit"),
            round(col("txn_avg"), 2).alias("txn_avg"),
            round(col("txn_min"), 2).alias("txn_min"),
            round(col("txn_max"), 2).alias("txn_max"),
            
            # Fraud & Risk Metrics
            col("fraud_rate"),
            col("fraud_amount_rate"),
            
            # Unique Counts
            col("unique_customers"),
            col("unique_merchants"),
            col("unique_cards"),
            col("unique_categories"),
            
            # Top Performers
            col("top_category"),
            col("top_category_cnt"),
            col("top_merchant_id"),
            col("top_merchant_name"),
            col("top_merchant_txn_cnt"),
            col("top_city"),
            col("top_city_revenue"),
            
            # Time-based Metrics
            col("peak_hour"),
            col("peak_hour_txn_cnt"),
            
            # Metadata
            col("created_at")
        )

    print("Writing to gold layer...")
    
    # Cache to avoid recomputation
    df_final.cache()
    record_count = df_final.count()
    print(f"Total date records: {record_count}")

    # Get list of dates being processed
    dates_list = df_final.select("txn_date").distinct().collect()
    print(f"Processing {len(dates_list)} dates")

    # Drop existing partitions for idempotent run
    for date_row in dates_list:
        date_value = date_row["txn_date"]
        try:
            spark.sql(f"""ALTER TABLE dlh_gold.transaction_summary 
                          DROP IF EXISTS PARTITION (txn_date='{date_value}')""")
            print(f"Dropped partition for {date_value}")
        except Exception as e:
            print(f"Partition drop skipped for {date_value}: {str(e)[:100]}")

    # Write to gold table (append mode with daily partitions)
    df_final \
        .write \
        .mode("append") \
        .partitionBy("txn_date") \
        .saveAsTable("dlh_gold.transaction_summary")
    
    # Unpersist cache
    df_final.unpersist()

    batch_end_time = datetime.now()
    print(f"Transform completed successfully in {(batch_end_time - batch_start_time).total_seconds():.2f} seconds")

except Exception as e:
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
