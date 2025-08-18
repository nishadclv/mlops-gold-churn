from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from src.common.logging import get_structured_logger
from src.common.config import settings

logger = get_structured_logger("bronze_layer")

def ingest_raw_data(spark: SparkSession, source_path: str) -> DataFrame:
    logger.info("Ingesting raw data", source_path=source_path)
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(source_path)
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_bronze_version", lit("v1.0"))
    )
    return df

def write_bronze_table(spark: SparkSession, df: DataFrame, table_name: str):
    full_table = f"{settings.catalog}.{settings.schema}.{table_name}"
    df.write.format("delta").mode("overwrite").saveAsTable(full_table)
    logger.info("Bronze table write complete", table_name=full_table)
