from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, year, month, count, sum as _sum, avg, max as _max, min as _min,
    lit, current_timestamp, countDistinct, to_date, datediff, abs as _abs,
    row_number
)
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import sys

print("Transform to user_detail gold layer")

# ---------------------------
# Create Spark session
spark = (SparkSession.builder
         .appName('Transform user_detail to gold layer')
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
# Create Gold database
spark.sql("CREATE DATABASE IF NOT EXISTS dlh_gold")

create_table = """
CREATE TABLE IF NOT EXISTS dlh_gold.user_detail (
    client_id               INT,
    client_key              INT,

    age_current             INT,
    age_retired             INT,
    birth_year              INT,
    birth_month             INT,
    gender                  STRING,
    
    income_per_capita       INT,
    income_yearly           INT,
    debt_total              INT,
    credit_score            INT,

    total_txn_amt           DECIMAL(18,2),
    total_pos_amt           DECIMAL(18,2),
    total_neg_amt           DECIMAL(18,2),
    
    total_txn               INT,
    total_txn_pos           INT,
    total_txn_neg           INT,
    total_txn_fraud         INT,
    total_txn_error         INT,

    lifetime_first_date     DATE,
    lifetime_last_date      DATE,
    lifetime_days           INT,

    merchant_unique_cnt     INT,
    merchant_top_id         INT,
    merchant_top_txn_cnt    INT,
    
    mcc_unique_cnt          INT,
    city_unique_cnt         INT,
    state_unique_cnt        INT,

    updated_at              TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);
"""
spark.sql(create_table)

try:
    print('='*60)
    print("Start transform user_detail")
    print('='*60)
    
    batch_start_time = datetime.now()

    # Read current users only
    df_users = spark.table("dlh_silver.users").filter(col("is_current") == True)
    
    # Read all transactions
    df_transactions = spark.table("dlh_gold.transaction_detail")
    
    user_count = df_users.count()
    txn_count = df_transactions.count()
    print(f"Active users count: {user_count}")
    print(f"Total transactions count: {txn_count}")
    
    
    print("\nAggregating transaction data per user...")
    
    df_txn_agg = df_transactions.groupBy("client_id").agg(
        # Transaction
        _sum("txn_amount").alias("total_txn_amt"),
        _sum(when(col("txn_amount") > 0, col("txn_amount")).otherwise(0)).alias("total_pos_amt"),
        _sum(when(col("txn_amount") < 0, _abs(col("txn_amount"))).otherwise(0)).alias("total_neg_amt"),

        count("*").alias("total_txn"),
        count(when(col("txn_amount") > 0, 1)).alias("total_txn_pos"),
        count(when(col("txn_amount") < 0, 1)).alias("total_txn_neg"),
        
        # Fraud and error counts
        count(when(col("is_fraud") == True, 1)).alias("total_txn_fraud"),
        count(when(col("is_error") == True, 1)).alias("total_txn_error"),
        
        # Lifetime dates
        _min(to_date(col("txn_date_time"))).alias("lifetime_first_date"),
        _max(to_date(col("txn_date_time"))).alias("lifetime_last_date"),
        
        # Diversity metrics
        countDistinct("merchant_id").alias("merchant_unique_cnt"),
        countDistinct("mcc").alias("mcc_unique_cnt"),
        countDistinct("merchant_city").alias("city_unique_cnt"),
        countDistinct("merchant_state").alias("state_unique_cnt")
    )
    
    print(f"Transaction aggregations completed: {df_txn_agg.count()} users with transactions")
    
    # Top merchant per user
    print("\nCalculating top merchant per user...")
    
    df_merchant_freq = df_transactions.groupBy("client_id", "merchant_id").agg(
        count("*").alias("txn_cnt_per_merchant")
    )
    
    window_top_merchant = Window.partitionBy("client_id").orderBy(col("txn_cnt_per_merchant").desc())
    
    df_top_merchant = df_merchant_freq \
        .withColumn("rn", row_number().over(window_top_merchant)) \
        .filter(col("rn") == 1) \
        .select(
            col("client_id").alias("client_id_merchant"),
            col("merchant_id").alias("merchant_top_id"),
            col("txn_cnt_per_merchant").alias("merchant_top_txn_cnt")
        )
    
    print(f"Top merchant calculation completed: {df_top_merchant.count()} users")
    
    # Join users with aggregations
    print("\nJoining user data with transaction aggregations...")
    
    df_user_detail = df_users.join(
        df_txn_agg,
        on="client_id",
        how="left"
    ).join(
        df_top_merchant,
        df_users["client_id"] == df_top_merchant["client_id_merchant"],
        how="left"
    ).drop("client_id_merchant")
    
    print("\nCalculating derived fields...")
    
    df_enriched = df_user_detail \
        .withColumn("age_current", 
                    year(current_timestamp()) - col("birth_year")) \
        .withColumn("lifetime_days",
                    when(col("lifetime_first_date").isNotNull() & col("lifetime_last_date").isNotNull(),
                         datediff(col("lifetime_last_date"), col("lifetime_first_date"))
                    ).otherwise(lit(0))) \
        .withColumn("updated_at", current_timestamp())
    
    # Select final schema matching the table DDL
    print("\nSelecting final schema...")
    
    df_gold = df_enriched.select(
        col("client_id"),
        col("client_key"),
        
        col("age_current"),
        col("retirement_age").alias("age_retired"),
        col("birth_year"),
        col("birth_month"),
        col("gender"),
        
        col("per_capita_income").cast("int").alias("income_per_capita"),
        col("yearly_income").cast("int").alias("income_yearly"),
        col("total_debt").cast("int").alias("debt_total"),
        col("credit_score"),
        
        col("total_txn_amt").cast(DecimalType(18,2)),
        col("total_pos_amt").cast(DecimalType(18,2)),
        col("total_neg_amt").cast(DecimalType(18,2)),
        
        col("total_txn").cast("int"),
        col("total_txn_pos").cast("int"),
        col("total_txn_neg").cast("int"),
        col("total_txn_fraud").cast("int"),
        col("total_txn_error").cast("int"),
        
        col("lifetime_first_date"),
        col("lifetime_last_date"),
        col("lifetime_days").cast("int"),
        
        col("merchant_unique_cnt").cast("int"),
        col("merchant_top_id").cast("int"),
        col("merchant_top_txn_cnt").cast("int"),
        
        col("mcc_unique_cnt").cast("int"),
        col("city_unique_cnt").cast("int"),
        col("state_unique_cnt").cast("int"),
        
        col("updated_at")
    ).fillna({
        "total_txn_amt": 0.0,
        "total_pos_amt": 0.0,
        "total_neg_amt": 0.0,
        "total_txn": 0,
        "total_txn_pos": 0,
        "total_txn_neg": 0,
        "total_txn_fraud": 0,
        "total_txn_error": 0,
        "lifetime_days": 0,
        "merchant_unique_cnt": 0,
        "merchant_top_id": 0,
        "merchant_top_txn_cnt": 0,
        "mcc_unique_cnt": 0,
        "city_unique_cnt": 0,
        "state_unique_cnt": 0
    })
    
    record_count = df_gold.count()
    print(f"\nUser detail records to write: {record_count}")
    
    # Write to Gold Layer
    print(f"Writing user details to gold layer...")
    df_gold.write \
        .mode("overwrite") \
        .saveAsTable("dlh_gold.user_detail")
    
    batch_end_time = datetime.now()
    processing_time = (batch_end_time - batch_start_time).total_seconds()
    
    print("\n" + "="*60)
    print("USER DETAIL TRANSFORMATION SUMMARY")
    print("="*60)
    print(f"Total users processed: {user_count:,}")
    print(f"Users with transactions: {df_txn_agg.count():,}")
    print(f"Total processing time: {processing_time:.2f} seconds")
    print(f"Average time per user: {processing_time/record_count*1000:.2f} ms")
    print("\nTransform completed successfully!")

except Exception as e:
    print(f"\n-- FATAL ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()