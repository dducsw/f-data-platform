from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, monotonically_increasing_id,
    lit, lead, upper, sha2, concat_ws
)
from pyspark.sql.window import Window
from datetime import datetime
import sys

print("Transform merchants to silver layer (SCD2 from bronze CDC)")

spark = (SparkSession.builder
            .appName('Transform table "merchants" from bronze to silver layer (SCD2)')
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
CREATE TABLE IF NOT EXISTS dlh_silver.merchants (
    merchant_key        STRING,
    merchant_id         STRING,
    merchant_name       STRING,
    category            STRING,
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
    print("Start transform table 'merchants'")

    # Read bronze CDC data
    df_bronze = spark.table("lake_bronze.merchants")
    print("Reading bronze data...")

    # Transform data - standardize
    df = (df_bronze
        .withColumn("merchant_name", upper(col("merchant_name")))
        .withColumn("category", upper(col("category")))
    )

    # SCD2 window
    window_spec = Window.partitionBy("merchant_id").orderBy(col("load_at").asc())

    df_scd2 = (df
        .withColumn("valid_from", col("load_at"))
        .withColumn("valid_to", lead("load_at").over(window_spec))
        .withColumn("is_current", when(col("valid_to").isNull(), lit(True)).otherwise(lit(False)))
        .select(
            sha2(concat_ws("|", col("merchant_id"), col("load_at")), 256).alias("merchant_key"),
            col("merchant_id"),
            col("merchant_name"),
            col("category"),
            col("valid_from"),
            col("valid_to"),
            col("is_current")
        )
    )

    # Split data into expired and active
    df_to_expire = df_scd2.filter(col("is_current") == False)
    df_new_active = df_scd2.filter(col("is_current") == True)

    # Read active data from silver
    df_active_old = spark.table("dlh_silver.merchants").filter(col("is_current") == True)
    df_active_unchanged = df_active_old.join(df_new_active, "merchant_id", "left_anti")
    df_active_final = df_new_active.unionByName(df_active_unchanged)

    # Append expired
    if df_to_expire.count() > 0:
        df_to_expire.write \
            .mode("append") \
            .format("parquet") \
            .partitionBy("is_current") \
            .bucketBy(40, "merchant_id") \
            .sortBy("merchant_id") \
            .saveAsTable("dlh_silver.merchants")

    # Overwrite active
    if df_active_final.count() > 0:
        df_active_final.write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy("is_current") \
            .bucketBy(40, "merchant_id") \
            .sortBy("merchant_id") \
            .saveAsTable("dlh_silver.merchants")

    batch_end_time = datetime.now()
    print(f"Transform completed in {(batch_end_time - batch_start_time).total_seconds():.2f} seconds")

except Exception as e:
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
