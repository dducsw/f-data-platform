from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, year, month, dayofmonth, hour,
    lit, current_timestamp, expr, coalesce, to_timestamp
)
from datetime import datetime
import sys

print("Transform transaction_details to gold layer (denormalized)")

spark = (SparkSession.builder
         .appName('Create transaction_details - denormalized fact table')
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
CREATE TABLE IF NOT EXISTS dlh_gold.transaction_details (
    -- Transaction identifiers
    txn_num                     STRING,

    -- Customer info
    customer_key                STRING,
    customer_id                 STRING,
    customer_gender             STRING,
    customer_city               STRING,
    customer_state              STRING,
    customer_lat                DOUBLE,
    customer_long               DOUBLE,
    customer_city_pop           INT,
    customer_job                STRING,
    customer_dob_year           INT,

    -- Card info
    card_key                    STRING,
    card_id                     STRING,
    card_brand                  STRING,
    card_type                   STRING,
    card_credit_limit           DOUBLE,
    card_is_expired             BOOLEAN,
    card_is_active              BOOLEAN,

    -- Merchant info
    merchant_key                STRING,
    merchant_id                 STRING,
    merchant_name               STRING,
    merchant_category           STRING,
    merch_lat                   DOUBLE,
    merch_long                  DOUBLE,

    -- Transaction details
    txn_date                    STRING,
    txn_time                    STRING,
    txn_datetime                TIMESTAMP,
    txn_category                STRING,
    txn_amount                  DOUBLE,
    txn_type                    STRING,
    is_fraud                    INT,

    -- Derived metrics
    txn_spending_ratio          DOUBLE,

    -- Metadata
    partition                   STRING,
    created_at                  TIMESTAMP
)
USING PARQUET
PARTITIONED BY (partition)
OPTIONS ('compression'='snappy')
"""
spark.sql(create_table_sql)

try:
    batch_start_time = datetime.now()
    print("Start creating denormalized transaction_details")

    # Read silver tables
    print("Reading silver layer tables...")
    df_transactions = spark.table("dlh_silver.transactions")
    df_customers = spark.table("dlh_silver.customers")
    df_cards = spark.table("dlh_silver.cards")
    df_merchants = spark.table("dlh_silver.merchants")

    # Join transactions with customers (Point-in-Time)
    print("Joining transactions with customers...")
    df_cust_dim = df_customers.select(
        col("customer_key"),
        col("customer_id").alias("cust_id_dim"),
        col("valid_from").alias("cust_valid_from"),
        col("valid_to").alias("cust_valid_to"),
        col("gender").alias("customer_gender"),
        col("city").alias("customer_city"),
        col("state").alias("customer_state"),
        col("lat").alias("customer_lat"),
        col("long").alias("customer_long"),
        col("city_pop").alias("customer_city_pop"),
        col("job").alias("customer_job"),
        col("dob_year").alias("customer_dob_year")
    )

    df_with_customers = df_transactions.join(
        df_cust_dim,
        (df_transactions.customer_id == df_cust_dim.cust_id_dim) & 
        (df_transactions.txn_datetime >= df_cust_dim.cust_valid_from) & 
        (df_transactions.txn_datetime < coalesce(df_cust_dim.cust_valid_to, to_timestamp(lit("9999-12-31"), "yyyy-MM-dd"))),
        how="left"
    ).drop("cust_id_dim", "cust_valid_from", "cust_valid_to")

    # Join with cards (Point-in-Time)
    print("Joining with cards...")
    df_card_dim = df_cards.select(
        col("card_key"),
        col("card_id").alias("card_id_dim"),
        col("valid_from").alias("card_valid_from"),
        col("valid_to").alias("card_valid_to"),
        col("card_brand"),
        col("card_type"),
        col("credit_limit").alias("card_credit_limit"),
        col("is_expired").alias("card_is_expired"),
        col("is_active").alias("card_is_active")
    )

    df_with_cards = df_with_customers.join(
        df_card_dim,
        (df_with_customers.card_id == df_card_dim.card_id_dim) & 
        (df_with_customers.txn_datetime >= df_card_dim.card_valid_from) & 
        (df_with_customers.txn_datetime < coalesce(df_card_dim.card_valid_to, to_timestamp(lit("9999-12-31"), "yyyy-MM-dd"))),
        how="left"
    ).drop("card_id_dim", "card_valid_from", "card_valid_to")

    # Join with merchants (Point-in-Time)
    print("Joining with merchants...")
    df_merch_dim = df_merchants.select(
        col("merchant_key"),
        col("merchant_id").alias("merch_id_dim"),
        col("valid_from").alias("merch_valid_from"),
        col("valid_to").alias("merch_valid_to"),
        col("merchant_name").alias("merchant_name_dim"),
        col("category").alias("merchant_category")
    )

    df_with_merchants = df_with_cards.join(
        df_merch_dim,
        (df_with_cards.merchant_id == df_merch_dim.merch_id_dim) & 
        (df_with_cards.txn_datetime >= df_merch_dim.merch_valid_from) & 
        (df_with_cards.txn_datetime < coalesce(df_merch_dim.merch_valid_to, to_timestamp(lit("9999-12-31"), "yyyy-MM-dd"))),
        how="left"
    ).drop("merch_id_dim", "merch_valid_from", "merch_valid_to")

    # Calculate derived metrics and select final columns
    print("Calculating derived metrics...")
    df_gold = df_with_merchants \
        .withColumn("txn_spending_ratio",
            when(col("card_credit_limit") > 0,
                 col("txn_amount") / col("card_credit_limit"))
            .otherwise(lit(0))
        ) \
        .withColumn("created_at", current_timestamp()) \
        .select(
            # Transaction identifiers
            col("txn_num"),

            # Customer info
            col("customer_key"),
            col("customer_id"),
            col("customer_gender"),
            col("customer_city"),
            col("customer_state"),
            col("customer_lat"),
            col("customer_long"),
            col("customer_city_pop"),
            col("customer_job"),
            col("customer_dob_year"),

            # Card info
            col("card_key"),
            col("card_id"),
            col("card_brand"),
            col("card_type"),
            col("card_credit_limit"),
            col("card_is_expired"),
            col("card_is_active"),

            # Merchant info
            col("merchant_key"),
            col("merchant_id"),
            when(col("merchant_name_dim").isNotNull(), col("merchant_name_dim"))
            .otherwise(col("merchant_name")).alias("merchant_name"),
            col("merchant_category"),
            col("merch_lat"),
            col("merch_long"),

            # Transaction details
            col("txn_date"),
            col("txn_time"),
            col("txn_datetime"),
            col("txn_category"),
            col("txn_amount"),
            col("txn_type"),
            col("is_fraud"),

            # Derived metrics
            # col("txn_spending_ratio"),

            # Metadata
            col("partition"),
            col("created_at")
        )

    print("Writing to gold layer...")

    # Write to gold table partitioned by date partition
    # Tối ưu: repartition theo partition key, append mode
    df_gold \
        .repartition(8, "partition") \
        .write \
        .mode("append") \
        .partitionBy("partition") \
        .option("maxRecordsPerFile", 500000) \
        .saveAsTable("dlh_gold.transaction_details")

    batch_end_time = datetime.now()
    print(f"Transform completed successfully in {(batch_end_time - batch_start_time).total_seconds():.2f} seconds")

except Exception as e:
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
