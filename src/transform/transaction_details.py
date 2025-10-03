from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, year, month, dayofmonth, hour, regexp_replace,
    lit, current_timestamp, sum as _sum, count as _count, to_date, regexp_extract
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
from datetime import datetime, timedelta
import sys

print("Transform transactions to gold layer")

# ---------------------------
# # Batch configuration
BATCH_START_DATE = "2019-02-01"
BATCH_END_DATE = "2019-03-07"# datetime.now().strftime("%Y-%m-%d")
BATCH_DAYS = 7

# ---------------------------
# Create Spark session
spark = (SparkSession.builder
         .appName('Transform table "transactions" from silver to gold layer')
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
# ---------------------------
# Create Gold table if not exists
create_table_sql = """
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
    txn_amount                  DECIMAL(18,4),
    txn_spending_ratio          DECIMAL(10,4),
    txn_use_chip                BOOLEAN,

    -- Date/Time
    txn_date_time               TIMESTAMP,
    txn_hour                    INT,

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
    created_at                  TIMESTAMP
)
PARTITIONED BY (txn_year INT, txn_month INT, txn_day INT)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);
"""
spark.sql(create_table_sql)

# ---------------------------
def generate_date_ranges(start_date, end_date, days_interval):
    """Generate date ranges for batch processing by days (e.g., weekly = 7 days)"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    date_ranges = []
    current = start
    
    while current <= end:
        # Calculate batch end (current + days_interval - 1)
        batch_end = current + timedelta(days=days_interval - 1)
        if batch_end > end:
            batch_end = end
            
        date_ranges.append((current.strftime("%Y-%m-%d"), batch_end.strftime("%Y-%m-%d")))
        
        # Move to next batch (start of next period)
        current = batch_end + timedelta(days=1)
    
    return date_ranges

def debug_column_type(df, col_name, step_name):
    print(f"\n[DEBUG] {step_name}")
    df.printSchema()
    df.select(col_name).show(5, truncate=False)
    print("-"*50)


# ---------------------------
def process_batch(batch_start, batch_end):
    """Process a single batch of transactions"""
    print(f"\n{'='*60}")
    print(f"Processing batch: {batch_start} to {batch_end}")
    print(f"{'='*60}")
    
    batch_start_time = datetime.now()
    
    # Read tables with date filter
    df_transactions = spark.table("dlh_silver.transactions") \
        .filter((to_date(col("date_time")) >= batch_start) & 
                (to_date(col("date_time")) <= batch_end))
    
    df_users = spark.table("dlh_silver.users")
    df_cards = spark.table("dlh_silver.cards")
    df_mcc = spark.table("dlh_bronze.mcc_codes")
    df_train_fraud_labels = spark.table("dlh_bronze.train_fraud_labels")
    

    batch_count = df_transactions.count()
    print(f"Batch transactions count: {batch_count}")
    
    if batch_count == 0:
        print("No transactions in this batch. Skipping...")
        return 0
    
    # Transform batch - Join with MCC codes
    df_with_mcc = df_transactions.join(
        df_mcc.select(
            col("mcc_code").alias("mcc"),
            col("description").alias("mcc_description")
        ),
        on="mcc",
        how="left"
    )
    
    # Join with fraud labels
    df_with_labels = df_with_mcc.join(
        df_train_fraud_labels.select(
            col("transaction_id"),
            col("is_fraud")
        ),
        on="transaction_id",
        how="left"
    )
    
    # Join with users
    df_with_users = df_with_labels.join(
        df_users.select(
            col("client_id"), 
            col("client_key"), 
            col("birth_year"),
            col("latitude").alias("client_latitude"), 
            col("longitude").alias("client_longitude"),
            col("credit_score").alias("client_credit_score"),
            col("num_credit_cards").alias("client_num_credit_cards")
        ),
        on=["client_id", "client_key"],
        how="left"
    )
    
    # Join with cards
    df_with_cards = df_with_users.join(
        df_cards.select(
            col("card_id"), 
            col("card_key"), 
            col("card_brand"), 
            col("card_type"),
            col("acct_open_year").alias("card_open_year"),
            col("expires_year").alias("card_expires_year"),
            col("credit_limit").alias("card_credit_limit"),
            col("card_on_dark_web")
        ),
        on=["card_id", "card_key"],
        how="left"
    )
    
        # Enrich data with calculated fields
    df_enriched = df_with_cards \
        .withColumn("client_age", 
                    year(col("date_time")) - col("birth_year")) \
        .withColumn("txn_spending_ratio", 
                    when(col("card_credit_limit") > 0,
                         (col("amount") / col("card_credit_limit")).cast(DecimalType(10, 4))
                    ).otherwise(lit(0))) \
        .withColumn("card_on_dark_web",
                    when(col("card_on_dark_web") == "Yes", lit(True))
                    .when(col("card_on_dark_web") == "No", lit(False))
                    .otherwise(lit(False))) \
        .withColumn("is_fraud_flag",
                    when(col("is_fraud") == "YES", lit(True))
                    .when(col("is_fraud").isNull(), lit(False))
                    .otherwise(lit(False))) \
        .withColumn("created_at", current_timestamp())
    
    # Select final schema
    df_gold = df_enriched.select(
        col("transaction_id"),
        col("client_key"),
        col("client_id"),
        col("client_age"),
        col("client_latitude"),
        col("client_longitude"),
        col("client_credit_score"),
        col("client_num_credit_cards"),
        col("card_key"),
        col("card_id"),
        col("card_brand"),
        col("card_type"),
        col("card_open_year"),
        col("card_expires_year"),
        col("card_credit_limit"),
        col("card_on_dark_web"),
        col("amount").alias("txn_amount"),
        col("txn_spending_ratio"),
        col("use_chip").alias("txn_use_chip"),
        col("date_time").alias("txn_date_time"),
        year(col("date_time")).alias("txn_year"),
        month(col("date_time")).alias("txn_month"),
        dayofmonth(col("date_time")).alias("txn_day"),
        hour(col("date_time")).alias("txn_hour"),
        col("merchant_id"),
        col("merchant_city"),
        col("merchant_state"),
        col("zip").alias("merchant_zip"),
        col("mcc").cast("int").alias("mcc"),
        col("mcc_description"),
        col("is_error"),
        col("is_fraud_flag").alias("is_fraud"),
        col("created_at")
    )
    
    # Get partition values for this batch
    
    partitions = df_gold.select("txn_year", "txn_month", "txn_day").distinct().collect()
    
    # Drop existing partitions to enable overwrite
    print(f"Dropping existing partitions for date range: {batch_start} to {batch_end}")
    for partition in partitions:
        try:
            spark.sql(f"""
                ALTER TABLE dlh_gold.transaction_detail 
                DROP IF EXISTS PARTITION (
                    txn_year={partition.txn_year}, 
                    txn_month={partition.txn_month}, 
                    txn_day={partition.txn_day}
                )
            """)
        except Exception as e:
            print(f"Warning: Could not drop partition {partition}: {e}")

    df_gold.printSchema() 

    # Write batch to Gold Layer
    print(f"Writing batch to gold layer...")
    df_gold.write \
        .mode("append") \
        .partitionBy("txn_year", "txn_month", "txn_day") \
        .saveAsTable("dlh_gold.transaction_detail")
    # batch đầu iên phaỉ overwrite sau đó các batch sau chỉ cần append
    batch_end_time = datetime.now()
    processing_time = (batch_end_time - batch_start_time).total_seconds()
    
    print(f"Batch completed in {processing_time:.2f} seconds")
    print(f"Records processed: {batch_count}")
    
    return batch_count

# ---------------------------
# Main execution
try:
    print('='*60)
    print('Start transform table "transactions" with time series batching')
    print('='*60)
    
    total_start_time = datetime.now()
    
    # Generate date ranges
    date_ranges = generate_date_ranges(BATCH_START_DATE, BATCH_END_DATE, BATCH_DAYS)
    print(f"\nTotal batches to process: {len(date_ranges)}")
    print(f"Batch interval: {BATCH_DAYS} days (~ {BATCH_DAYS/7:.1f} week(s))")
    
    # Process each batch
    total_records = 0
    successful_batches = 0
    failed_batches = []
    
    for idx, (batch_start, batch_end) in enumerate(date_ranges, 1):
        try:
            print(f"\n[Batch {idx}/{len(date_ranges)}]")
            records_processed = process_batch(batch_start, batch_end)
            total_records += records_processed
            successful_batches += 1
        except Exception as e:
            print(f"ERROR in batch {batch_start} to {batch_end}: {e}")
            import traceback
            traceback.print_exc()
            failed_batches.append((batch_start, batch_end, str(e)))
            continue
    
    total_end_time = datetime.now()
    total_processing_time = (total_end_time - total_start_time).total_seconds()
    
    # Summary
    print("\n" + "="*60)
    print("TRANSFORMATION SUMMARY")
    print("="*60)
    print(f"Total batches: {len(date_ranges)}")
    print(f"Successful batches: {successful_batches}")
    print(f"Failed batches: {len(failed_batches)}")
    print(f"Total records processed: {total_records:,}")
    print(f"Total processing time: {total_processing_time:.2f} seconds")
    print(f"Average time per batch: {total_processing_time/len(date_ranges):.2f} seconds")
    
    if failed_batches:
        print("\nFailed batches:")
        for batch_start, batch_end, error in failed_batches:
            print(f"  - {batch_start} to {batch_end}: {error}")
        sys.exit(1)
    else:
        print("\nAll batches completed successfully!")

except Exception as e:
    print(f"\n-- FATAL ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()