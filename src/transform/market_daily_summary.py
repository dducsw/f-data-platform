from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, year, month, dayofmonth, hour,
    lit, current_timestamp, sum as _sum, count as _count, to_date,
    avg, max as _max, min as _min, coalesce, countDistinct, row_number,
    abs as _abs, date_add
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
from datetime import datetime, timedelta
import sys

print('Transform to market_daily_summary gold layer')

# Create Spark session
spark = (SparkSession.builder
         .appName('Transform market_daily_summary to gold layer')
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
# Batch configuration
BATCH_START_DATE = "2019-02-01"
BATCH_END_DATE = "2019-02-07"
BATCH_DAYS = 7

# ---------------------------
# Create Gold database
spark.sql("CREATE DATABASE IF NOT EXISTS dlh_gold")

create_table = """
CREATE TABLE IF NOT EXISTS dlh_gold.market_daily_summary (
    period_date             DATE,

    
    txn_total_amt           DECIMAL(20,2),
    txn_pos_amt             DECIMAL(20,2),
    txn_neg_amt             DECIMAL(20,2),
    txn_avg_amt             DECIMAL(12,2),

    txn_total_cnt           INT,
    txn_pos_cnt             INT,
    txn_neg_cnt             INT,
    txn_fraud_cnt           INT,
    txn_error_cnt           INT,

    prev_1d_amt             DECIMAL(20,2),
    prev_7d_amt             DECIMAL(20,2),
    prev_30d_amt            DECIMAL(20,2),
    prev_1d_cnt             INT,
    prev_7d_cnt             INT,
    prev_30d_cnt            INT,

    client_unique_cnt       INT,
    client_new_cnt          INT,

    merchant_active_cnt     INT,
    merchant_top_mcc        INT,
    merchant_top_mcc_cnt    INT,

    peak_hour               INT,
    peak_hour_cnt           INT,

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

def generate_date_ranges(start_date, end_date, days_interval):
    """Generate date ranges for batch processing"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    date_ranges = []
    current = start

    while current <= end:
        batch_end = current + timedelta(days=days_interval-1)
        if batch_end > end:
            batch_end = end
        
        date_ranges.append((current.strftime("%Y-%m-%d"), batch_end.strftime("%Y-%m-%d")))
        current = batch_end + timedelta(days=1)
    
    return date_ranges

# ---------------------------
def process_batch(batch_start, batch_end):
    """Process a single batch - DE ONLY transformations"""
    print(f"\n{'='*60}")
    print(f"Processing batch: {batch_start} to {batch_end}")
    print(f"{'='*60}")
    
    batch_start_time = datetime.now()
    
    # Read transactions for batch period
    df_transactions = spark.table("dlh_gold.transaction_detail") \
        .filter((to_date(col("txn_date_time")) >= batch_start) & 
                (to_date(col("txn_date_time")) <= batch_end))
    
    batch_count = df_transactions.count()
    print(f"Batch transactions count: {batch_count}")
    
    if batch_count == 0:
        print("No transactions in this batch. Skipping...")
        return 0
    
    # DE RESPONSIBILITY: Basic daily aggregations
    df_daily = df_transactions \
        .withColumn("period_date", to_date(col("txn_date_time"))) \
        .groupBy("period_date") \
        .agg(
            
            _sum("txn_amount").alias("txn_total_amt"),
            _sum(when(col("txn_amount") >= 0, col("txn_amount")).otherwise(0)).alias("txn_pos_amt"),
            _sum(when(col("txn_amount") < 0, _abs(col("txn_amount"))).otherwise(0)).alias("txn_neg_amt"),
            avg("txn_amount").alias("txn_avg_amt"),
            
            
            _count("transaction_id").alias("txn_total_cnt"),
            _count(when(col("txn_amount") >= 0, 1)).alias("txn_pos_cnt"),
            _count(when(col("txn_amount") < 0, 1)).alias("txn_neg_cnt"),
            
            _count(when(col("is_fraud") == True, 1)).alias("txn_fraud_cnt"),
            _count(when(col("is_error") == True, 1)).alias("txn_error_cnt"),
            
            countDistinct("client_id").alias("client_unique_cnt"),
            countDistinct("merchant_id").alias("merchant_active_cnt")
        )
    
    # Peak hour calculation (per date)
    df_hourly = df_transactions \
        .withColumn("period_date", to_date(col("txn_date_time"))) \
        .groupBy("period_date", "txn_hour") \
        .agg(_count("transaction_id").alias("hour_txn_cnt"))
    
    window_peak = Window.partitionBy("period_date").orderBy(col("hour_txn_cnt").desc())
    df_peak_hour = df_hourly \
        .withColumn("rn", row_number().over(window_peak)) \
        .filter(col("rn") == 1) \
        .select(
            "period_date", 
            col("txn_hour").alias("peak_hour"),
            col("hour_txn_cnt").alias("peak_hour_cnt")
        )
    
    # Top MCC calculation (per date)
    df_mcc_daily = df_transactions \
        .withColumn("period_date", to_date(col("txn_date_time"))) \
        .groupBy("period_date", "mcc") \
        .agg(_count("transaction_id").alias("mcc_txn_cnt"))
    
    window_mcc = Window.partitionBy("period_date").orderBy(col("mcc_txn_cnt").desc())
    df_top_mcc = df_mcc_daily \
        .withColumn("rn", row_number().over(window_mcc)) \
        .filter(col("rn") == 1) \
        .select(
            "period_date", 
            col("mcc").alias("merchant_top_mcc"),
            col("mcc_txn_cnt").alias("merchant_top_mcc_cnt")
        )
    
    # New clients calculation
    df_client_first = spark.table("dlh_gold.transaction_detail") \
        .groupBy("client_id") \
        .agg(_min(to_date(col("txn_date_time"))).alias("first_txn_date"))
    
    df_new_clients = df_client_first \
        .filter((col("first_txn_date") >= batch_start) & 
                (col("first_txn_date") <= batch_end)) \
        .groupBy("first_txn_date") \
        .agg(_count("client_id").alias("client_new_cnt")) \
        .withColumnRenamed("first_txn_date", "period_date")
    
    
    # Get historical transaction data
    lookback_start = (datetime.strptime(batch_start, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
    
    df_historical = spark.table("dlh_gold.transaction_detail") \
        .filter((to_date(col("txn_date_time")) >= lookback_start) & # lookcback_start (-1d, -7d -30d) <= txn_date_time <= batch_start
                (to_date(col("txn_date_time")) <= batch_end)) \
        .withColumn("hist_date", to_date(col("txn_date_time"))) \
        .groupBy("hist_date") \
        .agg(
            _sum("txn_amount").alias("hist_amt"),
            _count("transaction_id").alias("hist_cnt")
        )

    # Debug: Check historical data
    print(f"Historical data count: {df_historical.count()}")
    print("Historical data sample:")
    df_historical.orderBy("hist_date").show(10)
    
    print("Current batch data:")
    df_daily.orderBy("period_date").show(10)
    
    # Join current data with historical data using date arithmetic
    df_with_history = df_daily
    
    # Join 1 day ago
    df_with_history = df_with_history.join(
        df_historical.selectExpr(
            "hist_date",
            "hist_amt as prev_1d_amt",
            "hist_cnt as prev_1d_cnt"
        ),
        col("period_date") == date_add(col("hist_date"), 1),
        "left"
    ).drop("hist_date")
    
    # Join 7 days ago
    df_with_history = df_with_history.join(
        df_historical.selectExpr(
            "hist_date",
            "hist_amt as prev_7d_amt",
            "hist_cnt as prev_7d_cnt"
        ),
        col("period_date") == date_add(col("hist_date"), 7),
        "left"
    ).drop("hist_date")
    
    # Join 30 days ago
    df_with_history = df_with_history.join(
        df_historical.selectExpr(
            "hist_date",
            "hist_amt as prev_30d_amt",
            "hist_cnt as prev_30d_cnt"
        ),
        col("period_date") == date_add(col("hist_date"), 30),
        "left"
    ).drop("hist_date")
    
    # Join all metrics together
    df_summary = df_with_history \
        .join(df_peak_hour, on="period_date", how="left") \
        .join(df_top_mcc, on="period_date", how="left") \
        .join(df_new_clients, on="period_date", how="left")
    
    # Add metadata
    df_enriched = df_summary.withColumn("updated_at", current_timestamp())
    
    # Select final schema - NO BUSINESS CALCULATIONS
    df_gold = df_enriched.select(
        col("period_date"),
        
        # Transaction amounts
        col("txn_total_amt").cast(DecimalType(20,2)),
        col("txn_pos_amt").cast(DecimalType(20,2)),
        col("txn_neg_amt").cast(DecimalType(20,2)),
        col("txn_avg_amt").cast(DecimalType(12,2)),
        
        # Transaction counts
        col("txn_total_cnt").cast("int"),
        col("txn_pos_cnt").cast("int"),
        col("txn_neg_cnt").cast("int"),
        col("txn_fraud_cnt").cast("int"),
        col("txn_error_cnt").cast("int"),
        
        # Historical data for DA to calculate growth
        coalesce(col("prev_1d_amt"), lit(0)).cast(DecimalType(20,2)).alias("prev_1d_amt"),
        coalesce(col("prev_7d_amt"), lit(0)).cast(DecimalType(20,2)).alias("prev_7d_amt"),
        coalesce(col("prev_30d_amt"), lit(0)).cast(DecimalType(20,2)).alias("prev_30d_amt"),
        coalesce(col("prev_1d_cnt"), lit(0)).cast("int").alias("prev_1d_cnt"),
        coalesce(col("prev_7d_cnt"), lit(0)).cast("int").alias("prev_7d_cnt"),
        coalesce(col("prev_30d_cnt"), lit(0)).cast("int").alias("prev_30d_cnt"),
        
        # Client metrics
        col("client_unique_cnt").cast("int"),
        coalesce(col("client_new_cnt"), lit(0)).cast("int").alias("client_new_cnt"),
        
        # Merchant metrics
        col("merchant_active_cnt").cast("int"),
        coalesce(col("merchant_top_mcc"), lit(0)).cast("int").alias("merchant_top_mcc"),
        coalesce(col("merchant_top_mcc_cnt"), lit(0)).cast("int").alias("merchant_top_mcc_cnt"),
        
        # Time metrics
        coalesce(col("peak_hour"), lit(0)).cast("int").alias("peak_hour"),
        coalesce(col("peak_hour_cnt"), lit(0)).cast("int").alias("peak_hour_cnt"),
        
        col("updated_at"),
        year(col("period_date")).alias("year"),
        month(col("period_date")).alias("month")
    ).fillna(0)
    
    record_count = df_gold.count()
    print(f"Daily summary records to write: {record_count}")
    
    if record_count > 0:
        # Get partition values for this batch
        partitions = df_gold.select("year", "month").distinct().collect()
        
        # Drop existing partitions
        print(f"Dropping existing partitions for date range: {batch_start} to {batch_end}")
        for partition in partitions:
            try:
                spark.sql(f"""
                    ALTER TABLE dlh_gold.market_daily_summary 
                    DROP IF EXISTS PARTITION (
                        year={partition.year}, 
                        month={partition.month}
                    )
                """)
            except Exception as e:
                print(f"Warning: Could not drop partition {partition}: {e}")

        # Write batch to Gold Layer
        print(f"Writing batch to gold layer...")
        df_gold.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .saveAsTable("dlh_gold.market_daily_summary")
    
    batch_end_time = datetime.now()
    processing_time = (batch_end_time - batch_start_time).total_seconds()
    
    print(f"Batch completed in {processing_time:.2f} seconds")
    print(f"Records processed: {record_count}")
    
    return record_count

# ---------------------------
# Main execution
try:
    print('='*60)
    print('Start transform market_daily_summary - DE ONLY')
    print('='*60)
    
    total_start_time = datetime.now()
    
    # Generate date ranges
    date_ranges = generate_date_ranges(BATCH_START_DATE, BATCH_END_DATE, BATCH_DAYS)
    print(f"\nTotal batches to process: {len(date_ranges)}")
    print(f"Batch interval: {BATCH_DAYS} days")
    
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
    print("MARKET DAILY SUMMARY TRANSFORMATION SUMMARY")
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