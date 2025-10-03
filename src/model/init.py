from pyspark.sql import SparkSession
from pathlib import Path
import logging
import sys
import re


# Log config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str) -> SparkSession:
    spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
            .config("spark.sql.hive.metastore.version", "4.0.1") \
            .config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("hive.metastore.uris", "thrift://localhost:9083") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("hive.exec.dynamic.partition", "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict") \
            .enableHiveSupport() \
            .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


            


def init_schema(spark: SparkSession, filename: str) -> tuple[int, int]:
    """
    Read SQL schema file and execute statements.
    Returns: (success_count, error_count)
    """


    schema_file_path = Path(__file__).resolve().parent / filename
    logger.info(f"Looking for schema definition file at {schema_file_path}")

    if not schema_file_path.exists():
        logger.error(f"Schema file not found: {schema_file_path}")
        return 0, 1

    success_count, error_count = 0, 0

    try:
        with open(schema_file_path, "r") as file:
            sql_text = file.read()
            # Remove single-line comments that start with -- (multiline mode)
            sql_no_comments = re.sub(r'--.*$', '', sql_text, flags=re.MULTILINE)
            # Split by semicolon and keep non-empty statements
            statements = [stmt.strip() for stmt in sql_no_comments.split(';') if stmt.strip()]

        logger.info(f"Found {len(statements)} SQL statements to execute.")

        total = len(statements)
        for i, stmt in enumerate(statements, start=1):
            logger.info(f"[{i}/{total}] Executing: {stmt[:150]}...")
            try:
                spark.sql(stmt)
                logger.info("++ Success")
                success_count += 1
            except Exception as e:
                logger.error(f"-- Error executing statement: {e}")
                error_count += 1

    except Exception as e:
        logger.exception(f"Fatal error while initializing schema: {e}")
        error_count += 1

    return success_count, error_count


def main(filename: str = "bronze.sql") -> None:
    """Entrypoint for spark-submit."""
    spark = create_spark_session("Initialize Bronze Schema")
    success, error = init_schema(spark, filename)

    logger.info(f"Execution completed. Success: {success}, Errors: {error}")
    spark.stop()

    # Exit code: 0 = success, 1 = failure (so Airflow detects task fail)
    sys.exit(0 if error == 0 else 1)


if __name__ == "__main__":
    filename_arg = sys.argv[1] if len(sys.argv) > 1 else "bronze.sql"
    main(filename_arg)
