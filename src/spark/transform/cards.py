from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, year, month, to_date, monotonically_increasing_id,
    lit, current_date, lead, upper, expr, sha2, concat_ws
)
from pyspark.sql.window import Window
from datetime import datetime
import sys

print("Transform cards to silver layer (SCD2 from bronze CDC)")

spark = (SparkSession.builder
            .appName('Transform table "cards" from bronze to silver layer (SCD2)')
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
            .config("spark.sql.hive.metastore.version", "4.0.1")
            .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*")
            .config("spark.sql.catalogImplementation", "hive")
            .config("hive.metastore.uris", "thrift://localhost:9083")
            .enableHiveSupport()
            .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

spark.sql("CREATE DATABASE IF NOT EXISTS lake_silver")

create_table_sql = """
CREATE TABLE IF NOT EXISTS dlh_silver.cards (
    card_key            STRING,
    card_id             STRING,
    customer_id         STRING,
    card_num            STRING,
    card_brand          STRING,
    card_type           STRING,
    credit_limit        DOUBLE,
    exp_date            STRING,
    exp_year            INT,
    exp_month           INT,
    is_expired          BOOLEAN,
    cvv                 STRING,
    cardholder_name     STRING,
    is_active           BOOLEAN,
    valid_from          TIMESTAMP,
    valid_to            TIMESTAMP,
    is_current          BOOLEAN
)
USING PARQUET
OPTIONS ('compression'='snappy')
"""
spark.sql(create_table_sql)

try:
    batch_start_time = datetime.now()
    print("Start transform table 'cards'")

    # Read full bronze (raw CDC data from Kafka, includes history)
    df_bronze = spark.table("lake_bronze.cards")
    print(f"Bronze records count: {df_bronze.count()}")


    # Transform data - standardize and enrich
    df = (df_bronze
        .withColumn("card_brand", upper(col("card_brand")))
        .withColumn("card_type", upper(col("card_type")))
        .withColumn("cardholder_name", upper(col("cardholder_name")))
        # Parse exp_date to extract year and month (format: YYYY-MM-DD or MM/YYYY)
        .withColumn("exp_date_parsed", 
            when(col("exp_date").contains("/"), 
                 to_date(col("exp_date"), "MM/yyyy"))
            .when(col("exp_date").contains("-"), 
                 to_date(col("exp_date"), "yyyy-MM-dd"))
            .otherwise(None)
        )
        .withColumn("exp_year", year(col("exp_date_parsed")))
        .withColumn("exp_month", month(col("exp_date_parsed")))
        # Check if card is expired
        .withColumn("is_expired", 
            when(col("exp_date_parsed").isNull(), lit(None))
            .when(col("exp_date_parsed") < current_date(), lit(True))
            .otherwise(lit(False))
        )
        .drop("exp_date_parsed")
    )

    # SCD2 window
    window_spec = Window.partitionBy("card_id").orderBy(col("load_at").asc())

    df_scd2 = (df
        .withColumn("valid_from", col("load_at"))
        .withColumn("valid_to", lead("load_at").over(window_spec))
        .withColumn("is_current", when(col("valid_to").isNull(), lit(True)).otherwise(lit(False)))
        .select(
            sha2(concat_ws("|", col("card_id"), col("load_at")), 256).alias("card_key"),
            col("card_id"),
            col("customer_id"),
            col("card_num"),
            col("card_brand"),
            col("card_type"),
            col("credit_limit"),
            col("exp_date"),
            col("exp_year"),
            col("exp_month"),
            col("is_expired"),
            col("cvv"),
            col("cardholder_name"),
            col("is_active"),
            col("valid_from"),
            col("valid_to"),
            col("is_current")
        )
    )

    print(f"Transform records count: {df_scd2.count()}")

    # Split data into expired and active
    df_to_expire = df_scd2.filter(col("is_current") == False)
    df_new_active = df_scd2.filter(col("is_current") == True)

    # Read active data from silver
    df_active_old = spark.table("dlh_silver.cards").filter(col("is_current") == True)
    df_active_unchanged = df_active_old.join(df_new_active, "card_id", "left_anti")
    df_active_final = df_new_active.unionByName(df_active_unchanged)

    # Append expired
    if df_to_expire.count() > 0:
        df_to_expire.write \
            .mode("append") \
            .format("parquet") \
            .partitionBy("is_current") \
            .bucketBy(40, "card_id") \
            .sortBy("card_id") \
            .saveAsTable("dlh_silver.cards")

    # Overwrite active
    if df_active_final.count() > 0:
        df_active_final.write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy("is_current") \
            .bucketBy(40, "card_id") \
            .sortBy("card_id") \
            .saveAsTable("dlh_silver.cards")

    batch_end_time = datetime.now()
    print(f"Transform completed in {(batch_end_time - batch_start_time).total_seconds():.2f} seconds")

except Exception as e:
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
