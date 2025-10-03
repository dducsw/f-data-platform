import sys
import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, MapType

input_file = '/home/ldduc/D/f-data-platform/data/json/train_fraud_labels.json'
output_file = '/home/ldduc/D/f-data-platform/data/json/train_fraud_labels_flat.jsonl'

def convert_json(input_path, output_path):
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    mapping = data.get("target", {})

    with open(output_path, "w", encoding="utf-8") as out:
        for k, v in mapping.items():
            record = {"transaction_id": int(k), "is_fraud": v}
            out.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"Converted {len(mapping)} records to {output_path}")

convert_json(input_file, output_file)

app_name = "Ingest File train_fraud_labels.json"
table_name = "dlh_bronze.train_fraud_labels"

spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
        .config("spark.sql.hive.metastore.version", "4.0.1")
        .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", "thrift://localhost:9083")
        .config("spark.jars", "/usr/local/spark/jars/postgresql-42.7.7.jar")
        # Memory optimization
        .config("spark.driver.memory", "8g")
        .config("spark.driver.maxResultSize", "4g")
        .config("spark.executor.memory", "8g")
        .config("spark.executor.cores", "2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .enableHiveSupport()
        .getOrCreate()
    )

spark.sparkContext.setLogLevel("ERROR")

create_table_query = f"""
CREATE TABLE IF NOT EXISTS dlh_bronze.train_fraud_labels (
    transaction_id   INT,
    is_fraud        STRING,
    file_name       STRING,
    load_at         TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);
"""
spark.sql(create_table_query)

file_path = 'file:///home/ldduc/D/f-data-platform/data/json/train_fraud_labels_flat.jsonl'
# Define the schema explicitly
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("is_fraud", StringType(), True)
])

# Read the JSON file with the defined schema
raw_data = spark.read.schema(schema).json(file_path).repartition(100)
print(raw_data)

df = (
    raw_data
    .withColumn("transaction_id", col("transaction_id").cast("int"))
    .withColumn("file_name", lit(os.path.basename(file_path)))
    .withColumn("load_at", current_timestamp())
    .repartition(100)
)

df.write.mode("overwrite").saveAsTable(table_name)


print(f"Data successfully ingested into table {table_name}.")
print(f"Record count: {df.count()}")

