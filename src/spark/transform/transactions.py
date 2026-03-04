from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, year, month, dayofmonth, hour,
    lit, current_timestamp, to_timestamp, to_date, upper, expr
)
from datetime import datetime
import sys

print("Transform transactions to silver layer from bronze CDC")

spark = (SparkSession.builder
         .appName('Transform table "transactions" from bronze to silver layer')
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

spark.sql("CREATE DATABASE IF NOT EXISTS dlh_silver")

create_table_sql = """
CREATE TABLE IF NOT EXISTS dlh_silver.transactions (
    txn_num             STRING,
    txn_date            STRING,
    txn_time            STRING,
    txn_datetime        TIMESTAMP,
    unix_time           BIGINT,
    txn_year            INT,
    txn_month           INT,
    txn_day             INT,
    txn_hour            INT,
    txn_category        STRING,
    txn_amount          DOUBLE,
    is_fraud            INT,
    merchant_id         STRING,
    merchant_name       STRING,
    merch_lat           DOUBLE,
    merch_long          DOUBLE,
    customer_id         STRING,
    card_id             STRING,
    txn_type            STRING,
    partition           STRING,
    load_at             TIMESTAMP
)
USING PARQUET
PARTITIONED BY (partition)
OPTIONS ('compression'='snappy')
"""
spark.sql(create_table_sql)

try:
    batch_start_time = datetime.now()
    print("Start transform table 'transactions'")

    # Read bronze CDC data
    df_bronze = spark.table("lake_bronze.transaction")
    print("Reading bronze data...")

    # Transform data - standardize and enrich
    df_transformed = (df_bronze
        # Combine date and time to create datetime
        .withColumn("txn_datetime", 
            to_timestamp(
                expr("concat(txn_date, ' ', txn_time)"), 
                "yyyy-MM-dd HH:mm:ss"
            )
        )
        
        # Extract date parts
        .withColumn("txn_year", year(col("txn_datetime")))
        .withColumn("txn_month", month(col("txn_datetime")))
        .withColumn("txn_day", dayofmonth(col("txn_datetime")))
        .withColumn("txn_hour", hour(col("txn_datetime")))
        
        # Standardize text fields (upper chỉ áp dụng cho non-null)
        .withColumn("txn_category", upper(col("txn_category")))
        .withColumn("merchant_name", upper(col("merchant_name")))
        .withColumn("txn_type", upper(col("txn_type")))
        
        # Select final columns
        .select(
            col("txn_num"),
            col("txn_date"),
            col("txn_time"),
            col("txn_datetime"),
            col("unix_time"),
            col("txn_year"),
            col("txn_month"),
            col("txn_day"),
            col("txn_hour"),
            col("txn_category"),
            col("txn_amount"),
            col("is_fraud"),
            col("merchant_id"),
            col("merchant_name"),
            col("merch_lat"),
            col("merch_long"),
            col("customer_id"),
            col("card_id"),
            col("txn_type"),
            col("partition"),
            col("load_at")
        )
    )

    print("Transforming and writing to silver layer...")

    # Write to silver table partitioned by date partition
    # Tối ưu: coalesce để giảm file nhỏ, append mode cho transaction fact table
    df_transformed \
        .repartition(8, "partition") \
        .write \
        .mode("append") \
        .partitionBy("partition") \
        .option("maxRecordsPerFile", 500000) \
        .saveAsTable("dlh_silver.transactions")
    
    print("Transform completed successfully!")
    batch_end_time = datetime.now()
    print(f"Transform completed in {(batch_end_time - batch_start_time).total_seconds():.2f} seconds")

except Exception as e:
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
