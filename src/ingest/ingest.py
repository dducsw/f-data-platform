import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


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

    # Usage: spark-submit transactions.py transactions dlh_bronze.transactions
    if len(sys.argv) != 3:
        logger.error("Usage: spark-submit transactions.py <source_table> <target_table>")
        sys.exit(1)

    source_table = sys.argv[1]
    target_table = sys.argv[2]

    jdbc_url = os.getenv("PG_JDBC_URL", "jdbc:postgresql://localhost:5432/finance_db")
    connection_properties = {
        "user": os.getenv("PG_USER", "postgres"),
        "password": os.getenv("PG_PASSWORD", "6666"),
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",  # Optimize JDBC fetch size
        "batchsize": "10000"   # Optimize batch size
    }

    spark = create_spark_session(f"Ingest-{source_table}")

    try:
        logger.info(f"Reading table `{source_table}` from PostgreSQL...")
        
        # Read with partitioning to avoid loading all data at once
        df = spark.read.jdbc(
            url=jdbc_url, 
            table=source_table, 
            properties=connection_properties,
        )
        df.printSchema()
        # Add required columns and optimize
        if source_table == "transactions":
            df_processed = (df
                .withColumn("load_at", current_timestamp())
                .withColumn("date_partition", date_format("date", "yyyy-MM"))
                .repartition(4)  # Optimize partitions
            )
        else:
            df_processed = (df
                .withColumn("load_at", current_timestamp())
            )

        logger.info(f"Schema of `{source_table}`:")
        df_processed.printSchema()

        total_rows = df_processed.count()
        logger.info(f"++ Total rows extracted: {total_rows}")

        logger.info(f"Writing data into Delta table `{target_table}`...")
        
        # Write in Delta format with partitioning
        df_processed.write.mode("append").saveAsTable(target_table)

        logger.info("++ Ingestion completed successfully!")

    except Exception as e:
        logger.exception(f"-- Error during ingestion of `{source_table}`")
        sys.exit(1)
    finally:
        logger.info("Stopping Spark session...")
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()