from pyspark.sql.functions import col, current_timestamp
from src.common.logging import get_structured_logger
from src.common.config import settings

logger = get_structured_logger("silver_layer")

def clean_bronze_data(spark, input_table, output_table):
    bronze = spark.table(input_table)
    silver = (
        bronze.filter(col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
        .withColumn("_processed_timestamp", current_timestamp())
    )
    silver.write.format("delta").mode("overwrite").saveAsTable(output_table)
    logger.info("Silver table ready", table=output_table)
