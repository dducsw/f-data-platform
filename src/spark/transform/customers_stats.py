from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, min, max, countDistinct,
    when, round, current_timestamp, datediff, lit
)
from pyspark.sql.window import Window
from datetime import datetime
import sys

print("Create customer statistics aggregation in gold layer")

spark = (SparkSession.builder
         .appName('Create customer_stats - customer metrics from transactions')
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
CREATE TABLE IF NOT EXISTS dlh_gold.customer_stats (
    customer_id                 STRING,
    
    -- Customer demographics (from dimension)
    customer_key                BIGINT,
    customer_ssn                STRING,
    customer_first_name         STRING,
    customer_last_name          STRING,
    customer_full_name          STRING,
    customer_gender             STRING,
    customer_street             STRING,
    customer_city               STRING,
    customer_state              STRING,
    customer_zip                STRING,
    customer_lat                DOUBLE,
    customer_long               DOUBLE,
    customer_city_pop           INT,
    customer_job                STRING,
    customer_dob                STRING,
    customer_dob_year           INT,
    customer_age                INT,
    customer_acct_num           STRING,
    
    -- Transaction count metrics
    txn_cnt                     BIGINT,
    txn_cnt_fraud               BIGINT,
    txn_cnt_legit               BIGINT,
    
    -- Transaction amount metrics
    txn_total                   DOUBLE,
    txn_total_fraud             DOUBLE,
    txn_total_legit             DOUBLE,
    txn_avg                     DOUBLE,
    txn_min                     DOUBLE,
    txn_max                     DOUBLE,
    
    -- Fraud metrics
    fraud_rate                  DOUBLE,
    fraud_amount_rate           DOUBLE,
    
    -- Activity metrics
    first_txn_date              STRING,
    last_txn_date               STRING,
    active_days                 BIGINT,
    customer_lifetime_days      INT,
    
    -- Card metrics
    unique_cards                BIGINT,
    
    -- Merchant metrics
    unique_merchants            BIGINT,
    
    -- Category metrics
    unique_categories           BIGINT,
    top_category                STRING,
    
    -- Metadata
    updated_at                  TIMESTAMP
)
USING PARQUET
OPTIONS ('compression'='snappy')
"""
spark.sql(create_table_sql)

try:
    batch_start_time = datetime.now()
    print("Start creating customer statistics")

    # Read silver transactions and current customers
    print("Reading silver layer tables...")
    df_transactions = spark.table("dlh_silver.transactions")
    df_customers = spark.table("dlh_silver.customers").filter(col("is_current") == True)

    # Aggregate transaction metrics by customer
    print("Aggregating transaction metrics...")
    df_txn_stats = df_transactions.groupBy("customer_id").agg(
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
        min("txn_date").alias("first_txn_date"),
        max("txn_date").alias("last_txn_date"),
        countDistinct("txn_date").alias("active_days"),
        
        # Card metrics
        countDistinct("card_id").alias("unique_cards"),
        
        # Merchant metrics
        countDistinct("merchant_id").alias("unique_merchants"),
        
        # Category metrics
        countDistinct("txn_category").alias("unique_categories")
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

    # Calculate customer lifetime days
    df_txn_stats = df_txn_stats \
        .withColumn("customer_lifetime_days",
            datediff(col("last_txn_date"), col("first_txn_date"))
        )

    # Find most frequent transaction category per customer
    print("Finding top categories...")
    
    
    category_window = Window.partitionBy("customer_id").orderBy(col("category_cnt").desc())
    
    df_top_category = df_transactions \
        .groupBy("customer_id", "txn_category") \
        .agg(count("*").alias("category_cnt")) \
        .withColumn("rank", 
            dense_rank().over(category_window)
        ) \
        .filter(col("rank") == 1) \
        .select(
            col("customer_id"),
            col("txn_category").alias("top_category")
        )

    # Join transaction stats with top category
    df_stats_with_category = df_txn_stats.join(
        df_top_category,
        on="customer_id",
        how="left"
    )

    # Join with customer dimension for demographics
    print("Joining with customer dimension...")
    df_final = df_stats_with_category.join(
        df_customers.select(
            col("customer_id"),
            col("customer_key"),
            col("ssn").alias("customer_ssn"),
            col("first_name").alias("customer_first_name"),
            col("last_name").alias("customer_last_name"),
            col("full_name").alias("customer_full_name"),
            col("gender").alias("customer_gender"),
            col("street").alias("customer_street"),
            col("city").alias("customer_city"),
            col("state").alias("customer_state"),
            col("zip").alias("customer_zip"),
            col("lat").alias("customer_lat"),
            col("long").alias("customer_long"),
            col("city_pop").alias("customer_city_pop"),
            col("job").alias("customer_job"),
            col("dob").alias("customer_dob"),
            col("dob_year").alias("customer_dob_year"),
            col("age").alias("customer_age"),
            col("acct_num").alias("customer_acct_num")
        ),
        on="customer_id",
        how="left"
    ) \
    .withColumn("updated_at", current_timestamp()) \
    .select(
        col("customer_id"),
        
        # Customer demographics
        col("customer_key"),
        col("customer_ssn"),
        col("customer_first_name"),
        col("customer_last_name"),
        col("customer_full_name"),
        col("customer_gender"),
        col("customer_street"),
        col("customer_city"),
        col("customer_state"),
        col("customer_zip"),
        col("customer_lat"),
        col("customer_long"),
        col("customer_city_pop"),
        col("customer_job"),
        col("customer_dob"),
        col("customer_dob_year"),
        col("customer_age"),
        col("customer_acct_num"),
        
        # Transaction count metrics
        col("txn_cnt"),
        col("txn_cnt_fraud"),
        col("txn_cnt_legit"),
        
        # Transaction amount metrics
        round(col("txn_total"), 2).alias("txn_total"),
        round(col("txn_total_fraud"), 2).alias("txn_total_fraud"),
        round(col("txn_total_legit"), 2).alias("txn_total_legit"),
        round(col("txn_avg"), 2).alias("txn_avg"),
        round(col("txn_min"), 2).alias("txn_min"),
        round(col("txn_max"), 2).alias("txn_max"),
        
        # Fraud metrics
        col("fraud_rate"),
        col("fraud_amount_rate"),
        
        # Activity metrics
        col("first_txn_date"),
        col("last_txn_date"),
        col("active_days"),
        col("customer_lifetime_days"),
        
        # Card metrics
        col("unique_cards"),
        
        # Merchant metrics
        col("unique_merchants"),
        
        # Category metrics
        col("unique_categories"),
        col("top_category"),
        
        # Metadata
        col("updated_at")
    )

    print("Writing to gold layer...")
    record_count = df_final.count()
    print(f"Total customers with transactions: {record_count}")

    # Write to gold table (overwrite mode for full refresh)
    df_final \
        .write \
        .mode("overwrite") \
        .saveAsTable("dlh_gold.customer_stats")

    batch_end_time = datetime.now()
    print(f"Transform completed successfully in {(batch_end_time - batch_start_time).total_seconds():.2f} seconds")

except Exception as e:
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
