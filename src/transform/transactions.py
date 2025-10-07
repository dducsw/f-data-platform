from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, year, month, dayofmonth, hour, regexp_replace,
    lit, current_timestamp, to_date
)
from pyspark.sql.types import DecimalType
from datetime import datetime, timedelta
import sys

print("Transform transactions to silver layer")

# ---------------------------
# Batch configuration
BATCH_START_DATE = "2019-01-01"
BATCH_END_DATE = "2019-01-30"# datetime.now().strftime("%Y-%m-%d")
BATCH_DAYS = 7  # batch size

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
    is_error            BOOLEAN,
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
def generate_date_ranges(start_date, end_date, days_interval):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_ranges = []
    current = start
    while current <= end:
        batch_end = current + timedelta(days=days_interval - 1)
        if batch_end > end:
            batch_end = end
        date_ranges.append((current.strftime("%Y-%m-%d"), batch_end.strftime("%Y-%m-%d")))
        current = batch_end + timedelta(days=1)
    return date_ranges

def process_batch(batch_start, batch_end):
    print(f"\n{'='*60}")
    print(f"Processing batch: {batch_start} to {batch_end}")
    print(f"{'='*60}")
    batch_start_time = datetime.now()

    # Read bronze and silver tables (lọc theo ngày batch)
    df_bronze = spark.table("dlh_bronze.transactions") \
        .filter((to_date(col("date_time")) >= batch_start) & (to_date(col("date_time")) <= batch_end))
    df_users = spark.table("dlh_silver.users").filter(col("is_current") == True)
    df_cards = spark.table("dlh_silver.cards").filter(col("is_current") == True)

    batch_count = df_bronze.count()
    print(f"Bronze records count: {batch_count}")
    if batch_count == 0:
        print("No transactions in this batch. Skipping...")
        return 0

    # Transform batch
    df_transformed = (df_bronze
            .withColumn("transaction_id", col("id"))
            .withColumn("amount_clean", regexp_replace(col("amount"), "[$,]", "").cast(DecimalType(18,4)))
            .withColumn("year", year(col("date_time")))
            .withColumn("month", month(col("date_time")))
            .withColumn("day", dayofmonth(col("date_time")))
            .withColumn("hour", hour(col("date_time")))
            .withColumn("use_chip", when(col("use_chip").isin(["Yes", "yes"]), True).otherwise(False))
            .withColumn("is_error",
                    when(col("errors").isNotNull(), lit(True))
                    .otherwise(lit(False))) \
            .withColumn("updated_at", current_timestamp())
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
            col("is_error"),
            col("updated_at")
        )

    # Drop existing partitions for this batch
    partitions = df_silver_batch.select("year", "month").distinct().collect()
    print(f"Dropping existing partitions for date range: {batch_start} to {batch_end}")
    for partition in partitions:
        try:
            spark.sql(f"""
                ALTER TABLE dlh_silver.transactions 
                DROP IF EXISTS PARTITION (
                    year={partition.year}, 
                    month={partition.month}
                )
            """)
        except Exception as e:
            print(f"Warning: Could not drop partition {partition}: {e}")

    # Write batch to Silver Layer
    df_silver_batch.write \
            .mode("append") \
            .partitionBy("year","month") \
            .saveAsTable("dlh_silver.transactions")

    batch_end_time = datetime.now()
    processing_time = (batch_end_time - batch_start_time).total_seconds()
    print(f"Batch completed in {processing_time:.2f} seconds")
    print(f"Records processed: {batch_count}")
    return batch_count

# ---------------------------
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