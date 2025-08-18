from src.common.config import settings
from src.common.spark_utils import get_spark_session
from src.data_engineering.bronze_layer import ingest_raw_data, write_bronze_table

def main():
    spark = get_spark_session()
    source_table = settings.source_table
    bronze_table = f"{settings.catalog}.{settings.schema}.bronze_customers"

    df = ingest_raw_data(spark, source_table)
    write_bronze_table(spark, df, "bronze_customers")

    print(f"Bronze ingestion complete: {bronze_table}, Rows: {df.count()}")
    return {"status": "success", "table": bronze_table, "rows": df.count()}

if __name__ == "__main__":
    main()
