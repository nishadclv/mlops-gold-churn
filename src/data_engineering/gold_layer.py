from pyspark.sql.functions import when, col, current_timestamp
from src.common.logging import get_structured_logger
from src.common.config import settings

logger = get_structured_logger("gold_layer")

def engineer_features(spark, input_table, output_table):
    silver = spark.table(input_table)
    gold = (
        silver
        .withColumn("churn_label", when(col("churn") == "Yes", 1).otherwise(0))
        .withColumn("avg_monthly_spend", col("total_charges") / col("tenure_months"))
        .withColumn("high_value_customer", when(col("total_charges") > 2000, 1).otherwise(0))
        .withColumn("long_tenure_customer", when(col("tenure_months") > 24, 1).otherwise(0))
        .withColumn("_feature_timestamp", current_timestamp())
    )
    gold.write.format("delta").mode("overwrite").saveAsTable(output_table)
    logger.info("Gold table ready", table=output_table)
