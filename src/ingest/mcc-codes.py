import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType



app_name = "Ingest File mcc-codes.json"
table_name = "dlh_bronze.mcc_codes"

spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
        .config("spark.sql.hive.metastore.version", "4.0.1")
        .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", "thrift://localhost:9083")
        .config("spark.jars", "/usr/local/spark/jars/postgresql-42.7.7.jar")
        .enableHiveSupport()
        .getOrCreate()
    )

spark.sparkContext.setLogLevel("ERROR")

create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    mcc_code    INT,
    description STRING,
    file_name   STRING,
    load_at     TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
)
"""
spark.sql(create_table_query)

file_path = 'file:///home/ldduc/D/f-data-platform/data/json/mcc_codes.json'
raw_data = spark.read.option("multiline","true").json(file_path)
data = [(int(k), v) for k, v in raw_data.collect()[0].asDict().items()]
schema = StructType([
    StructField("mcc_code", IntegerType(), True),
    StructField("description", StringType(), True)
])

df = spark.createDataFrame(data, schema=schema)

df = df.withColumn("file_name", lit(os.path.basename(file_path))) \
        .withColumn("load_at", current_timestamp())

df.printSchema()
print(df.head())
df.write.insertInto(table_name, overwrite=True)

print(f"Data successfully ingested into table {table_name}.")

print(f"Record count: {df.count()}")

