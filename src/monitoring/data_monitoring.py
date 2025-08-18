from src.common.logging import get_structured_logger

logger = get_structured_logger("data_monitoring")

def monitor_data_quality(spark, table):
    df = spark.table(table)
    row_count = df.count()
    null_stats = {col: df.filter(df[col].isNull()).count() for col in df.columns}
    logger.info("Data monitoring", row_count=row_count, null_counts=null_stats)
    return {"row_count": row_count, "null_stats": null_stats}
