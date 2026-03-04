from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, year, to_date, monotonically_increasing_id,
    lit, current_date, lead, upper, lower, sha2, concat_ws, expr
)
from pyspark.sql.window import Window
from datetime import datetime
import sys

print("Transform customers to silver layer (SCD2 from bronze CDC)")

spark = (SparkSession.builder
            .appName('Transform table "customers" from bronze to silver layer (SCD2)')
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
            .config("spark.sql.hive.metastore.version", "4.0.1")
            .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*")
            .config("spark.sql.catalogImplementation", "hive")
            .config("hive.metastore.uris", "thrift://localhost:9083")
            .enableHiveSupport()
            .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Enable dynamic partition overwrite mode
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

spark.sql("CREATE DATABASE IF NOT EXISTS dlh_silver")

create_table_sql = """
CREATE TABLE IF NOT EXISTS dlh_silver.customers (
    customer_key        STRING,
    customer_id         STRING,
    ssn                 STRING,
    first_name          STRING,
    last_name           STRING,
    full_name           STRING,
    gender              STRING,
    street              STRING,
    city                STRING,
    state               STRING,
    zip                 STRING,
    lat                 DOUBLE,
    long                DOUBLE,
    city_pop            INT,
    job                 STRING,
    dob                 STRING,
    dob_year            INT,
    age                 INT,
    acct_num            STRING,
    profile             STRING,
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
    print("Start transform table 'customers'")

    # Read full bronze (raw CDC data from Kafka, includes history)
    df_bronze = spark.table("lake_bronze.users")
    print(f"Bronze records count: {df_bronze.count()}")


    # Transform data - standardize and enrich
    df = (df_bronze
        .withColumn("first_name", upper(col("first_name")))
        .withColumn("last_name", upper(col("last_name")))
        .withColumn("full_name", 
            when(col("first_name").isNotNull() & col("last_name").isNotNull(),
                 expr("concat(first_name, ' ', last_name)"))
            .otherwise(None)
        )
        .withColumn("gender", upper(col("gender")))
        .withColumn("city", upper(col("city")))
        .withColumn("state", upper(col("state")))
        # Parse dob to extract year and calculate age
        .withColumn("dob_parsed", 
            when(col("dob").contains("-"), to_date(col("dob"), "yyyy-MM-dd"))
            .when(col("dob").contains("/"), to_date(col("dob"), "MM/dd/yyyy"))
            .otherwise(None)
        )
        .withColumn("dob_year", year(col("dob_parsed")))
        .withColumn("age", 
            when(col("dob_parsed").isNotNull(), 
                 year(current_date()) - year(col("dob_parsed")))
            .otherwise(None)
        )
        .drop("dob_parsed")
    )

    # SCD2 window
    window_spec = Window.partitionBy("customer_id").orderBy(col("load_at").asc())

    df_scd2 = (df
        .withColumn("valid_from", col("load_at"))
        .withColumn("valid_to", lead("load_at").over(window_spec))
        .withColumn("is_current", when(col("valid_to").isNull(), lit(True)).otherwise(lit(False)))
        .select(
            sha2(concat_ws("|", col("customer_id"), col("load_at")), 256).alias("customer_key"),
            col("customer_id"),
            col("ssn"),
            col("first_name"),
            col("last_name"),
            col("full_name"),
            col("gender"),
            col("street"),
            col("city"),
            col("state"),
            col("zip"),
            col("lat"),
            col("long"),
            col("city_pop"),
            col("job"),
            col("dob"),
            col("dob_year"),
            col("age"),
            col("acct_num"),
            col("profile"),
            col("valid_from"),
            col("valid_to"),
            col("is_current")
        )
    )

    print(f"Transform records count: {df_scd2.count()}")

    # Split data into 2 parts: expired and active
    df_to_expire = df_scd2.filter(col("is_current") == False)
    df_new_active = df_scd2.filter(col("is_current") == True)

    print(f"Expired records: {df_to_expire.count()}")
    print(f"Active records: {df_new_active.count()}")

    # Read active data from silver
    df_active_old = spark.table("dlh_silver.customers").filter(col("is_current") == True)

    # Join to get active data with not change
    df_active_unchanged = df_active_old.join(df_new_active, "customer_id", "left_anti")

    # Union with unchanged
    df_active_final = df_new_active.unionByName(df_active_unchanged)

    # Append when is_current = false
    if df_to_expire.count() > 0:
        df_to_expire.write \
            .mode("append") \
            .format("parquet") \
            .partitionBy("is_current") \
            .bucketBy(40, "customer_id") \
            .sortBy("customer_id") \
            .saveAsTable("dlh_silver.customers")

    # Overwrite when is_current = true
    if df_active_final.count() > 0:
        df_active_final.write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy("is_current") \
            .bucketBy(40, "customer_id") \
            .sortBy("customer_id") \
            .saveAsTable("dlh_silver.customers")

    batch_end_time = datetime.now()
    print(f"Transform completed in {(batch_end_time - batch_start_time).total_seconds():.2f} seconds")

except Exception as e:
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()