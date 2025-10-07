from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, year, month, to_date, regexp_replace, expr, 
    lit, row_number, current_date, monotonically_increasing_id, 
    split, concat, lpad, coalesce, regexp_extract, current_timestamp, lower, upper
)
from pyspark.sql.window import Window
from datetime import datetime
import sys

print("Transform cards to silver layer")

spark = (SparkSession.builder
            .appName('Transform table "cards" from bronze to silver layer')
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
            .config("spark.sql.hive.metastore.version", "4.0.1")
            .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*")
            .config("spark.sql.catalogImplementation", "hive")
            .config("hive.metastore.uris", "thrift://localhost:9083")
            .enableHiveSupport()
            .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

spark.sql("CREATE DATABASE IF NOT EXISTS dlh_silver")

create_table_sql = """
CREATE TABLE IF NOT EXISTS dlh_silver.cards (
    card_key            INT,
    card_id             INT,
    client_id           INT,
    card_brand          STRING,
    card_type           STRING,
    expires_date        STRING,
    expires_year        INT,
    expires_month       INT,
    is_expires          BOOLEAN,
    has_chip            BOOLEAN,
    num_cards_issued    INT,
    credit_limit        DECIMAL(12,2),
    acct_open_date      STRING,
    acct_open_year      INT,
    acct_open_month     INT,
    year_pin_last_changed INT,
    card_on_dark_web    STRING,
    valid_from          TIMESTAMP,
    valid_to            TIMESTAMP,
    is_current          BOOLEAN
)
USING PARQUET
OPTIONS ('compression'='snappy')
"""
spark.sql(create_table_sql)

try:
    print('Start transform table "cards"')
    batch_start_time = datetime.now()

    # Read bronze data
    df_bronze = spark.table("dlh_bronze.cards")
    print(f"Bronze records count: {df_bronze.count()}")


    # Transform data
    df = (df_bronze
        .withColumn("card_brand", upper(col("card_brand")))
        .withColumn("card_type", upper(col("card_type")))
        # Keep original expires as string
        .withColumn("expires_date", col("expires"))
        
        # Extract month and year using regex - SAFE approach
        .withColumn("expires_month_str", 
            regexp_extract(col("expires"), "^([0-9]{1,2})/[0-9]{4}$", 1)
        )
        .withColumn("expires_year_str", 
            regexp_extract(col("expires"), "^[0-9]{1,2}/([0-9]{4})$", 1)
        )
        
        # Convert to integers only if valid
        .withColumn("expires_month", 
            when((col("expires_month_str") != "") & 
                 (col("expires_month_str").cast("int") >= 1) & 
                 (col("expires_month_str").cast("int") <= 12),
                 col("expires_month_str").cast("int")
            ).otherwise(lit(None))
        )
        .withColumn("expires_year", 
            when((col("expires_year_str") != "") & 
                 (col("expires_year_str").cast("int") >= 1900) & 
                 (col("expires_year_str").cast("int") <= 2100),
                 col("expires_year_str").cast("int")
            ).otherwise(lit(None))
        )
        
        # Process acct_open_date similarly to expires
        .withColumn("acct_open_date_original", col("acct_open_date"))
        .withColumn("acct_open_month_str", 
            regexp_extract(col("acct_open_date"), "^([0-9]{1,2})/[0-9]{4}$", 1)
        )
        .withColumn("acct_open_year_str", 
            regexp_extract(col("acct_open_date"), "^[0-9]{1,2}/([0-9]{4})$", 1)
        )
        .withColumn("acct_open_month", 
            when((col("acct_open_month_str") != "") & 
                 (col("acct_open_month_str").cast("int") >= 1) & 
                 (col("acct_open_month_str").cast("int") <= 12),
                 col("acct_open_month_str").cast("int")
            ).otherwise(lit(None))
        )
        .withColumn("acct_open_year", 
            when((col("acct_open_year_str") != "") & 
                 (col("acct_open_year_str").cast("int") >= 1900) & 
                 (col("acct_open_year_str").cast("int") <= 2100),
                 col("acct_open_year_str").cast("int")
            ).otherwise(lit(None))
        )
        
        # Calculate current year/month for comparison
        .withColumn("current_year", year(current_date()))
        .withColumn("current_month", month(current_date()))
        
        # Check if expired using integer comparison (safer than date parsing)
        .withColumn("is_expires", 
            when((col("expires_year").isNull()) | (col("expires_month").isNull()), 
                 lit(None))
            .when((col("expires_year") < col("current_year")) |
                  ((col("expires_year") == col("current_year")) & 
                   (col("expires_month") < col("current_month"))), 
                  lit(True))
            .otherwise(lit(False))
        )
        
        # Clean credit limit
        .withColumn("credit_limit_clean", 
            when((col("credit_limit").isNotNull()) & (col("credit_limit") != ""),
                 regexp_replace(col("credit_limit"), "[^0-9.-]", "").cast("decimal(12,2)")
            ).otherwise(lit(None))
        )
        .withColumn("card_on_dark_web",
                    when(col("card_on_dark_web") == "Yes", lit(True))
                    .when(col("card_on_dark_web") == "No", lit(False))
                    .otherwise(lit(False))) \
        
        # Clean up temporary columns
        .drop("expires_month_str", "expires_year_str", "current_year", "current_month", 
              "acct_open_month_str", "acct_open_year_str")
    )

    # Select final columns
    df_transformed = df.select(
        col("id").alias("card_id"),
        col("client_id"),
        col("card_brand"),
        col("card_type"),
        col("expires_date"),
        col("expires_year"),
        col("expires_month"),
        col("is_expires"),
        col("has_chip"),
        col("num_cards_issued"),
        col("credit_limit_clean").alias("credit_limit"),
        col("acct_open_date_original").alias("acct_open_date"),
        col("acct_open_year"),
        col("acct_open_month"),
        col("year_pin_last_changed"),
        col("card_on_dark_web"),
        col("load_at").alias("valid_from"),
        lit(None).cast("timestamp").alias("valid_to"),
        lit(True).alias("is_current")
    )

    # Filter out bad records
    df_cleaned = df_transformed.filter(
        col("card_id").isNotNull() &
        col("client_id").isNotNull()
    )

    # Read data from silver layer
    df_silver = spark.table("dlh_silver.cards")
    print(f"Silver records count: {df_silver.count()}")

    # Generate card key for new records
    max_key = spark.table("dlh_silver.cards").agg({"card_key": "max"}).collect()[0][0] or 0
    df_cleaned = df_cleaned.withColumn("card_key", col("card_id") + lit(max_key + 1))

    # Mark existing records as outdated
    join_key = df_cleaned.select("card_id").distinct()
    df_silver_updated = df_silver.join(join_key, on="card_id", how="inner") \
        .filter(col("is_current") == True) \
        .withColumn("valid_to", lit(current_timestamp())) \
        .withColumn("is_current", lit(False))
    
    df_silver_unchanged = (df_silver
                          .join(join_key, on="card_id", how="left_anti"))
    
    # Combine all records for final result
    df_final = df_cleaned.unionByName(df_silver_updated).unionByName(df_silver_unchanged)

    print(f"Transform records count: {df_final.count()}")

    # Append to Silver
    df_final.write \
        .mode("append") \
        .saveAsTable("dlh_silver.cards")


    batch_end_time = datetime.now()
    processing_time = (batch_end_time - batch_start_time).total_seconds()

    print(f"Transform completed successfully in {processing_time:.2f} seconds")
    print(f"Records processed: {df_final.count()}")
    print(f"Records: {df_final.head(5)}")


except Exception as e:
    print(f"-- Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()