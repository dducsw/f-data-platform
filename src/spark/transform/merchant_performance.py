from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, min, max, countDistinct,
    when, round, current_timestamp, current_date, lit, desc, row_number, date_format
)
from pyspark.sql.window import Window
from datetime import datetime
import sys

print("Create merchant performance aggregation in gold layer")

spark = (SparkSession.builder
         .appName('Create merchant_performance - merchant metrics from transactions')
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
CREATE TABLE IF NOT EXISTS dlh_gold.merchant_performance (
    -- Merchant Identity
    merchant_id                 STRING,
    merchant_key                BIGINT,
    merchant_name               STRING,
    merchant_category           STRING,
    
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
    risk_level                  STRING,
    
    -- Customer Insights
    unique_customers            BIGINT,
    new_customers_cnt           BIGINT,
    returning_customer_rate     DOUBLE,
    avg_txn_per_customer        DOUBLE,
    top_customer_city           STRING,
    
    -- Activity Timeline
    first_txn_timestamp         TIMESTAMP,
    last_txn_timestamp          TIMESTAMP,
    active_days                 BIGINT,
    
    -- Temporal Context
    snapshot_date               DATE,
    
    -- Metadata
    created_at                  TIMESTAMP
)
USING PARQUET
PARTITIONED BY (snapshot_date)
OPTIONS ('compression'='snappy')
"""
spark.sql(create_table_sql)

try:
    batch_start_time = datetime.now()
    print("Start creating merchant performance metrics")

    # Read silver transactions and current merchants
    print("Reading silver layer tables...")
    df_transactions = spark.table("dlh_silver.transactions")
    df_merchants = spark.table("dlh_silver.merchants").filter(col("is_current") == True)
    df_customers = spark.table("dlh_silver.customers").filter(col("is_current") == True)

    # Aggregate transaction metrics by merchant
    print("Aggregating transaction metrics...")
    df_txn_stats = df_transactions.groupBy("merchant_id").agg(
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
        
        # Activity metrics
        min("txn_datetime").alias("first_txn_timestamp"),
        max("txn_datetime").alias("last_txn_timestamp"),
        countDistinct("txn_date").alias("active_days"),
        
        # Customer metrics
        countDistinct("customer_id").alias("unique_customers")
    )

    # Calculate fraud rates
    df_txn_stats = df_txn_stats \
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

    # Calculate avg_txn_per_customer and risk_level
    df_txn_stats = df_txn_stats \
        .withColumn("avg_txn_per_customer",
            round(
                when(col("unique_customers") > 0, col("txn_cnt") / col("unique_customers"))
                .otherwise(0),
                2
            )
        ) \
        .withColumn("risk_level",
            when(col("fraud_rate") >= 5.0, lit("High"))
            .when(col("fraud_rate") >= 2.0, lit("Medium"))
            .otherwise(lit("Low"))
        )

    # Calculate new customers (first transaction with merchant)
    print("Calculating new customer metrics...")
    
    # Get first transaction date per customer across all merchants
    customer_first_txn = df_transactions \
        .groupBy("customer_id") \
        .agg(min("txn_datetime").alias("customer_first_txn"))
    
    # Join to identify new customers per merchant
    df_txn_with_customer_first = df_transactions.join(
        customer_first_txn,
        on="customer_id",
        how="left"
    )
    
    # Count new customers per merchant (where merchant txn = customer's first txn)
    df_new_customers = df_txn_with_customer_first \
        .filter(col("txn_datetime") == col("customer_first_txn")) \
        .groupBy("merchant_id") \
        .agg(countDistinct("customer_id").alias("new_customers_cnt"))
    
    # Calculate returning customer rate
    df_txn_stats = df_txn_stats.join(
        df_new_customers,
        on="merchant_id",
        how="left"
    ).fillna(0, subset=["new_customers_cnt"]) \
    .withColumn("returning_customer_rate",
        round(
            when(col("unique_customers") > 0, 
                 (col("unique_customers") - col("new_customers_cnt")) / col("unique_customers") * 100)
            .otherwise(0),
            2
        )
    )

    # Find top customer city by revenue per merchant
    print("Finding top customer cities...")
    
    city_revenue_window = Window.partitionBy("merchant_id").orderBy(desc("city_revenue"))
    
    df_top_city = df_transactions \
        .join(
            df_customers.select(
                col("customer_id"),
                col("city").alias("customer_city")
            ),
            on="customer_id",
            how="left"
        ) \
        .groupBy("merchant_id", "customer_city") \
        .agg(sum("txn_amount").alias("city_revenue")) \
        .withColumn("rank", row_number().over(city_revenue_window)) \
        .filter(col("rank") == 1) \
        .select(
            col("merchant_id"),
            col("customer_city").alias("top_customer_city")
        )

    # Join transaction stats with top city
    df_stats_with_insights = df_txn_stats.join(
        df_top_city,
        on="merchant_id",
        how="left"
    )

    # Join with merchant dimension for merchant info
    print("Joining with merchant dimension...")
    df_final = df_stats_with_insights.join(
        df_merchants.select(
            col("merchant_id"),
            col("merchant_key"),
            col("merchant_name"),
            col("category").alias("merchant_category")
        ),
        on="merchant_id",
        how="left"
    ) \
    .withColumn("snapshot_date", current_date()) \
    .withColumn("created_at", current_timestamp()) \
    .select(
        # Merchant Identity
        col("merchant_id"),
        col("merchant_key"),
        col("merchant_name"),
        col("merchant_category"),
        
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
        col("risk_level"),
        
        # Customer Insights
        col("unique_customers"),
        col("new_customers_cnt"),
        col("returning_customer_rate"),
        col("avg_txn_per_customer"),
        col("top_customer_city"),
        
        # Activity Timeline
        col("first_txn_timestamp"),
        col("last_txn_timestamp"),
        col("active_days"),
        
        # Temporal Context
        col("snapshot_date"),
        
        # Metadata
        col("created_at")
    )

    print("Writing to gold layer...")
    
    # Cache df_final to avoid recomputation
    df_final.cache()
    record_count = df_final.count()
    print(f"Total merchants with transactions: {record_count}")

    # Get current date for partition
    current_snapshot = df_final.select(date_format(col("snapshot_date"), "yyyy-MM-dd")).first()[0]
    print(f"Snapshot date: {current_snapshot}")

    # Delete existing partition for today if exists (idempotent daily run)
    try:
        spark.sql(f"""ALTER TABLE dlh_gold.merchant_performance 
                      DROP IF EXISTS PARTITION (snapshot_date='{current_snapshot}')""")
        print(f"Dropped existing partition for {current_snapshot}")
    except Exception as e:
        print(f"Partition drop skipped (table may not exist yet): {str(e)[:100]}")

    # Write to gold table (append mode with daily snapshot partition)
    df_final \
        .write \
        .mode("append") \
        .partitionBy("snapshot_date") \
        .saveAsTable("dlh_gold.merchant_performance")
    
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

