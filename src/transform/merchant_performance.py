from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, year, month, count, sum as _sum, 
    lit, current_timestamp, countDistinct, to_date, coalesce,
    trunc, abs as _abs
)
from pyspark.sql.types import DecimalType
from datetime import datetime, timedelta
import sys

print("Transform to merchant_monthly_performance gold layer")

# ---------------------------
# Batch configuration
BATCH_START_DATE = "2019-02-01"
BATCH_END_DATE = "2019-03-07"

# ---------------------------
# Create Spark session
spark = (SparkSession.builder
         .appName('Transform merchant_monthly_performance to gold layer')
         .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
         .config("spark.sql.catalogImplementation", "hive")
         .config("hive.metastore.uris", "thrift://localhost:9083")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ---------------------------
# Create Gold database
spark.sql("CREATE DATABASE IF NOT EXISTS dlh_gold")

# Create table - Chỉ lưu RAW metrics, không tính toán phức tạp
create_table = """
CREATE TABLE IF NOT EXISTS dlh_gold.merchant_monthly_performance (
    merchant_id             INT,
    period_month            DATE,
    
    -- Geographic metrics
    active_city_cnt         INT,
    active_state_cnt        INT,
    active_mcc_cnt          INT,

    -- Revenue metrics (current month)
    rev_total               DECIMAL(18,2),
    rev_positive            DECIMAL(18,2),
    rev_negative            DECIMAL(18,2),
    
    -- Revenue metrics (previous month)
    prev_rev_total          DECIMAL(18,2),
    prev_rev_positive       DECIMAL(18,2),

    -- Transaction metrics (current month)
    txn_total_amt           DECIMAL(18,2),
    txn_total_cnt           INT,
    txn_positive_cnt        INT,
    txn_negative_cnt        INT,
    
    -- Transaction metrics (previous month)
    prev_txn_total_cnt      INT,
    prev_txn_positive_cnt   INT,

    -- Fraud metrics
    fraud_txn_cnt           INT,
    
    -- Error metrics  
    error_txn_cnt           INT,

    -- Client metrics
    unique_client_cnt       INT,
    new_client_cnt          INT,
    returning_client_cnt    INT,

    updated_at              TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY',
    'parquet.enable.dictionary'='true'
);
"""
spark.sql(create_table)

# ---------------------------
def generate_month_ranges(start_date, end_date):
    """Generate monthly date ranges from start to end date"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    date_ranges = []
    current = start.replace(day=1)
    
    while current <= end:
        if current.month == 12:
            month_end = current.replace(day=31)
        else:
            next_month = current.replace(month=current.month + 1, day=1)
            month_end = next_month - timedelta(days=1)
        
        if month_end > end:
            month_end = end
            
        date_ranges.append((current.strftime("%Y-%m-%d"), month_end.strftime("%Y-%m-%d")))
        
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1, day=1)
        else:
            current = current.replace(month=current.month + 1, day=1)
        
        if current > end:
            break
    
    return date_ranges

# ---------------------------
def process_batch(batch_start, batch_end):
    """Process a single batch of merchant performance"""
    print(f"\n{'='*60}")
    print(f"Processing batch: {batch_start} to {batch_end}")
    print(f"{'='*60}")
    
    batch_start_time = datetime.now()
    
    # Read transactions for the batch period
    df_transactions = spark.table("dlh_gold.transaction_detail") \
        .filter((to_date(col("txn_date_time")) >= batch_start) & 
                (to_date(col("txn_date_time")) <= batch_end))
    
    txn_count = df_transactions.count()
    print(f"Transactions count: {txn_count}")
    
    if txn_count == 0:
        print("No transactions in this batch. Skipping...")
        return 0
    
    # Add period_month column
    df_with_period = df_transactions \
        .withColumn("period_month", trunc(col("txn_date_time"), "month"))
    
    # Get previous month data
    prev_month_start = (datetime.strptime(batch_start, "%Y-%m-%d") - timedelta(days=1)).replace(day=1)
    prev_month_end = datetime.strptime(batch_start, "%Y-%m-%d") - timedelta(days=1)
    
    df_prev_month = spark.table("dlh_gold.transaction_detail") \
        .filter((to_date(col("txn_date_time")) >= prev_month_start.strftime("%Y-%m-%d")) & 
                (to_date(col("txn_date_time")) <= prev_month_end.strftime("%Y-%m-%d")))
    
    
    df_current_agg = df_with_period.groupBy("merchant_id", "period_month").agg(
        # Geographic diversity
        countDistinct("merchant_city").alias("active_city_cnt"),
        countDistinct("merchant_state").alias("active_state_cnt"),
        countDistinct("mcc").alias("active_mcc_cnt"),
        
        # Revenue
        _sum("txn_amount").alias("rev_total"),
        _sum(when(col("txn_amount") > 0, col("txn_amount")).otherwise(0)).alias("rev_positive"),
        _sum(when(col("txn_amount") < 0, _abs(col("txn_amount"))).otherwise(0)).alias("rev_negative"),
        
        # Transaction metrics
        _sum(_abs(col("txn_amount"))).alias("txn_total_amt"),
        count("*").alias("txn_total_cnt"),
        count(when(col("txn_amount") > 0, 1)).alias("txn_positive_cnt"),
        count(when(col("txn_amount") < 0, 1)).alias("txn_negative_cnt"),
        
        # Fraud metrics
        count(when(col("is_fraud") == True, 1)).alias("fraud_txn_cnt"),
        
        # Error metrics  
        count(when(col("is_error") == True, 1)).alias("error_txn_cnt"),
        
        # Client metrics
        countDistinct("client_id").alias("unique_client_cnt")
    )
    
    # Aggregate previous month
    df_prev_agg = df_prev_month.groupBy("merchant_id").agg(
        _sum("txn_amount").alias("prev_rev_total"),
        _sum(when(col("txn_amount") > 0, col("txn_amount")).otherwise(0)).alias("prev_rev_positive"),
        count("*").alias("prev_txn_total_cnt"),
        count(when(col("txn_amount") > 0, 1)).alias("prev_txn_positive_cnt")
    )
    
    # Calculate new vs returning clients
    df_client_history = spark.table("dlh_gold.transaction_detail") \
        .filter(to_date(col("txn_date_time")) < batch_start) \
        .select("merchant_id", "client_id").distinct()
    
    df_current_clients = df_with_period \
        .select("merchant_id", "client_id") \
        .distinct()
    
    df_client_analysis = df_current_clients.alias("curr").join(
        df_client_history.alias("hist"),
        on=["merchant_id", "client_id"],
        how="left"
    ).groupBy("curr.merchant_id").agg(
        count(when(col("hist.client_id").isNull(), 1)).alias("new_client_cnt"),
        count(when(col("hist.client_id").isNotNull(), 1)).alias("returning_client_cnt")
    ).withColumnRenamed("merchant_id", "merchant_id_clients")
    
    # Join all aggregations
    df_performance = df_current_agg.join(
        df_prev_agg,
        on="merchant_id",
        how="left"
    ).join(
        df_client_analysis,
        df_current_agg["merchant_id"] == df_client_analysis["merchant_id_clients"],
        how="left"
    ).drop("merchant_id_clients")
    
    df_enriched = df_performance.withColumn("updated_at", current_timestamp())
    
    # Select final schema
    df_gold = df_enriched.select(
        col("merchant_id"),
        col("period_month"),
        
        # Geographic
        col("active_city_cnt"),
        col("active_state_cnt"),
        col("active_mcc_cnt"),
        
        # Revenue - current
        col("rev_total").cast(DecimalType(18, 2)),
        col("rev_positive").cast(DecimalType(18, 2)),
        col("rev_negative").cast(DecimalType(18, 2)),
        
        # Revenue - previous
        coalesce(col("prev_rev_total"), lit(0)).cast(DecimalType(18, 2)).alias("prev_rev_total"),
        coalesce(col("prev_rev_positive"), lit(0)).cast(DecimalType(18, 2)).alias("prev_rev_positive"),
        
        # Transaction - current
        col("txn_total_amt").cast(DecimalType(18, 2)),
        col("txn_total_cnt"),
        col("txn_positive_cnt"),
        col("txn_negative_cnt"),
        
        # Transaction - previous
        coalesce(col("prev_txn_total_cnt"), lit(0)).alias("prev_txn_total_cnt"),
        coalesce(col("prev_txn_positive_cnt"), lit(0)).alias("prev_txn_positive_cnt"),
        
        # Fraud & Error
        col("fraud_txn_cnt"),
        col("error_txn_cnt"),
        
        # Client
        col("unique_client_cnt"),
        coalesce(col("new_client_cnt"), lit(0)).alias("new_client_cnt"),
        coalesce(col("returning_client_cnt"), lit(0)).alias("returning_client_cnt"),
        
        col("updated_at"),
        year(col("period_month")).alias("year"),
        month(col("period_month")).alias("month")
    ).fillna(0)
    
    record_count = df_gold.count()
    print(f"Merchant performance records to write: {record_count}")
    
    if record_count > 0:
        partitions = df_gold.select("year", "month").distinct().collect()
        
        print(f"Dropping existing partitions for period: {batch_start} to {batch_end}")
        for partition in partitions:
            try:
                spark.sql(f"""
                    ALTER TABLE dlh_gold.merchant_monthly_performance 
                    DROP IF EXISTS PARTITION (
                        year={partition.year}, 
                        month={partition.month}
                    )
                """)
            except Exception as e:
                print(f"Warning: Could not drop partition {partition}: {e}")
        
        print(f"Writing merchant performance to gold layer...")
        df_gold.write \
            .mode("append") \
            .partitionBy("year", "month") \
            .saveAsTable("dlh_gold.merchant_monthly_performance")
    
    batch_end_time = datetime.now()
    processing_time = (batch_end_time - batch_start_time).total_seconds()
    
    print(f"Batch completed in {processing_time:.2f} seconds")
    print(f"Records processed: {record_count}")
    
    return record_count

# ---------------------------
# Main execution
try:
    print('='*60)
    print('Start transform merchant_monthly_performance - DE ONLY')
    print('='*60)
    
    total_start_time = datetime.now()
    
    month_ranges = generate_month_ranges(BATCH_START_DATE, BATCH_END_DATE)
    print(f"\nTotal monthly batches to process: {len(month_ranges)}")
    
    total_records = 0
    successful_batches = 0
    failed_batches = []
    
    for idx, (batch_start, batch_end) in enumerate(month_ranges, 1):
        try:
            print(f"\n[Batch {idx}/{len(month_ranges)}]")
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
    
    print("\n" + "="*60)
    print("MERCHANT PERFORMANCE TRANSFORMATION SUMMARY")
    print("="*60)
    print(f"Total batches: {len(month_ranges)}")
    print(f"Successful batches: {successful_batches}")
    print(f"Failed batches: {len(failed_batches)}")
    print(f"Total merchant records processed: {total_records:,}")
    print(f"Total processing time: {total_processing_time:.2f} seconds")
    print(f"Average time per batch: {total_processing_time/len(month_ranges):.2f} seconds")
    
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