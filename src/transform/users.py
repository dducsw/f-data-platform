from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, year, month, to_date, regexp_replace,
    lit, row_number, current_timestamp, monotonically_increasing_id, lead
)
from pyspark.sql.window import Window
from datetime import datetime
import sys, os


print("Transform crm_cust_info to silver layer")

spark = (SparkSession.builder
            .appName('Transform table "users" from bronze to silver layer')
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
            .config("spark.sql.hive.metastore.version", "4.0.1")
            .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*")
            .config("spark.sql.catalogImplementation", "hive")
            .config("hive.metastore.uris", "thrift://localhost:9083")
            .enableHiveSupport()
            .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace,
    lit, row_number, current_timestamp, monotonically_increasing_id, lead
)
from pyspark.sql.window import Window
from datetime import datetime
import sys, os


print("Transform crm_cust_info to silver layer")

spark = (SparkSession.builder
            .appName('Transform table "users" from bronze to silver layer')
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
            .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Create database and table if not exist
spark.sql("CREATE DATABASE IF NOT EXISTS dlh_silver")

create_table_sql = """
CREATE TABLE IF NOT EXISTS dlh_silver.users (
    client_key          INT,
    client_id           INT,
    current_age         INT,
    retirement_age      INT,
    birth_year          INT,
    birth_month         INT,
    age_group           STRING,
    gender              STRING,
    address             STRING,
    latitude            DECIMAL(9,6),
    longitude           DECIMAL(9,6),
    per_capita_income   DECIMAL(15,2),
    yearly_income       DECIMAL(15,2),
    income_group        STRING,
    total_debt          DECIMAL(15,2),
    credit_score        INT,
    num_credit_cards    INT,
    valid_from          TIMESTAMP,
    valid_to            TIMESTAMP,
    is_current          BOOLEAN
)
USING PARQUET
OPTIONS ('compression'='snappy')
"""
spark.sql(create_table_sql)

try:
    print('Start transform table "users"')
    batch_start_time = datetime.now()

    # Read table from bronze layer
    df_bronze = spark.table("dlh_bronze.users")
    print(f"Bronze records count: {df_bronze.count()}")

    # -----------------------------
    # Clean & transform
    df = (df_bronze
        # Derive age group
        .withColumn("current_age", year(current_timestamp()) - col("birth_year"))
        .withColumn("age_group", when(col("current_age") < 18, "Under 18")
                                  .when((col("current_age") >= 18) & (col("current_age") < 35), "18-34")
                                  .when((col("current_age") >= 35) & (col("current_age") < 50), "35-49")
                                  .when((col("current_age") >= 50) & (col("current_age") < 65), "50-64")
                                  .otherwise("65+"))
        # Clean income fields
        .withColumn("per_capita_income_clean", regexp_replace(col("per_capita_income"), "[$,]", "").cast("decimal(15,2)"))
        .withColumn("yearly_income_clean", regexp_replace(col("yearly_income"), "[$,]", "").cast("decimal(15,2)"))
        .withColumn("total_debt_clean", regexp_replace(col("total_debt"), "[$,]", "").cast("decimal(15,2)"))

        # Derive income group
        .withColumn("income_group", when(col("yearly_income_clean") < 30000, "Low Income")
                                     .when((col("yearly_income_clean") >= 30000) & (col("yearly_income_clean") < 70000), "Middle Income")
                                     .otherwise("High Income"))
    )

    # Apply SCD2 logic
    window_spec = Window.partitionBy("id").orderBy(col("load_at").asc())  # Replace `client_id` with `id`

    df_scd2 = (df
        .withColumn("valid_from", col("load_at"))
        .withColumn("valid_to", lead("load_at").over(window_spec))
        .withColumn("is_current", when(col("valid_to").isNull(), lit(True)).otherwise(lit(False)))
        .select(
            monotonically_increasing_id().cast("int").alias("client_key"),
            col("id").alias("client_id"),  # Replace `client_id` with `id`
            col("current_age"), # Need to calc again to exactly with year now
            col("retirement_age"),
            col("birth_year"),
            col("birth_month"),
            col("age_group"),
            col("gender"),
            col("address"),
            col("latitude"),
            col("longitude"),
            col("per_capita_income_clean").alias("per_capita_income"),
            col("yearly_income_clean").alias("yearly_income"),
            col("income_group"),
            col("total_debt_clean").alias("total_debt"),
            col("credit_score"),
            col("num_credit_cards"),
            col("valid_from"),
            col("valid_to"),
            col("is_current")
        )
    )

    print(f"Transform records count: {df_scd2.count()}")

    # Append to Silver
    df_scd2.write \
        .mode("overwrite") \
        .option("path", "hdfs://localhost:9000/user/hive/warehouse/dlh_silver.db/users") \
        .saveAsTable("dlh_silver.users")

    batch_end_time = datetime.now()
    processing_time = (batch_end_time - batch_start_time).total_seconds()

    print(f"Transform completed successfully in {processing_time:.2f} seconds")
    print(f"Records processed: {df_scd2.count()}")

except Exception as e:
    print(f"-- Error: {e}")
    sys.exit(1)