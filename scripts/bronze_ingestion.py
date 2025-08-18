from src.data_engineering.bronze_layer import ingest_raw_data, write_bronze_table
from src.common.spark_utils import get_spark_session
from src.common.config import settings

def main():
    spark = get_spark_session()
    df = ingest_raw_data(spark, source_path=settings.catalog + "/bronze_data.csv")  # Set real path
    write_bronze_table(spark, df, "bronze_customers")

if __name__ == "__main__":
    main()
