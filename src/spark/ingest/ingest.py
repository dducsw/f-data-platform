import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, max as spark_max
from datetime import datetime


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# == Configuration ==
# PostgreSQL connection settings
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "6666")

PG_CONNECTION_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
    "fetchsize": "10000",
    "batchsize": "10000"
}

# Tables with incremental load support (have updated_at column)
INCREMENTAL_TABLES = ["customers", "cards", "merchants"]


def get_max_updated_at(spark: SparkSession, output_path: str, table_name: str) -> str:
    """
    Get the maximum updated_at value from existing data in Data Lake.
    Returns None if path doesn't exist or has no data.
    """
    try:
        # Check if output path exists and has data
        df_existing = spark.read.format("parquet").load(output_path)
        if "updated_at" in df_existing.columns:
            max_value = df_existing.select(spark_max("updated_at")).collect()[0][0]
            if max_value:
                logger.info(f"Found max updated_at for {table_name}: {max_value}")
                return max_value
        return None
    except Exception as e:
        logger.info(f"No existing data found at {output_path}, performing full load.")
        return None


def create_spark_session(app_name: str) -> SparkSession:
    """Create SparkSession with optimized memory settings."""
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
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    """Main function for spark-submit job."""


    # Usage: spark-submit ingest.py <source_table> <source_database> <target_database> <target_table>
    if len(sys.argv) != 5:
        logger.error("Usage: spark-submit ingest.py <source_table> <source_database> <target_database> <target_table>")
        sys.exit(1)

    SOURCE_TABLE = sys.argv[1]
    SOURCE_DATABASE = sys.argv[2]
    TARGET_DATABASE = sys.argv[3]
    TARGET_TABLE = sys.argv[4]

    OUTPUT_PATH = f"/user/hive/warehouse/{TARGET_DATABASE}.db/{SOURCE_TABLE}"

    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{SOURCE_DATABASE}"
    connection_properties = PG_CONNECTION_PROPERTIES

    spark = create_spark_session(f"Ingest-{SOURCE_TABLE}")

    try:
        # Check for incremental load support
        is_incremental = SOURCE_TABLE in INCREMENTAL_TABLES
        max_updated_at = None
        
        if is_incremental:
            max_updated_at = get_max_updated_at(spark, OUTPUT_PATH, SOURCE_TABLE)
        
        # Build query for incremental load if applicable
        if is_incremental and max_updated_at:
            # Incremental load: only fetch records with updated_at > max_value
            query = f"(SELECT * FROM {SOURCE_TABLE} WHERE updated_at > '{max_updated_at}') AS incremental_data"
            logger.info(f"Reading incremental data from `{SOURCE_TABLE}` where updated_at > {max_updated_at}...")
        else:
            # Full load
            query = SOURCE_TABLE
            logger.info(f"Reading full table `{SOURCE_TABLE}` from PostgreSQL...")

        # Read data from PostgreSQL
        df = spark.read.jdbc(
            url=jdbc_url,
            table=query,
            properties=connection_properties,
        )
        df.printSchema()
        
        # Check if incremental load returned no data
        if df.count() == 0:
            logger.info(f"No new records to load for {SOURCE_TABLE}. Exiting.")
            spark.stop()
            return
        
        # Add required columns and optimize
        if SOURCE_TABLE == "transactions":
            df_processed = (df
                .withColumn("load_at", current_timestamp())
                .withColumn("date_partition", date_format("date", "yyyy-MM"))
                .repartition(4)  # Optimize partitions
            )
        else:
            df_processed = (df
                .withColumn("load_at", current_timestamp())
            )

        logger.info(f"Schema of `{SOURCE_TABLE}`:")
        df_processed.printSchema()

        total_rows = df_processed.count()
        logger.info(f"++ Total rows extracted: {total_rows}")

        logger.info(f"Writing data into Data Lake path: {OUTPUT_PATH} ...")

        # Write as Parquet to OUTPUT_PATH (partition if exists)
        writer = df_processed.write.mode("append").format("parquet")
        if "date_partition" in df_processed.columns:
            writer = writer.partitionBy("date_partition")
        writer.save(OUTPUT_PATH)

        logger.info("++ Ingestion completed successfully!")

    except Exception as e:
        logger.exception(f"-- Error during ingestion of `{SOURCE_TABLE}`")
        sys.exit(1)
    finally:
        logger.info("Stopping Spark session...")
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()