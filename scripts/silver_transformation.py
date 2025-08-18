from src.data_engineering.silver_layer import clean_bronze_data
from src.common.spark_utils import get_spark_session
from src.common.config import settings

def main():
    spark = get_spark_session()
    input_table = f"{settings.catalog}.{settings.schema}.bronze_customers"
    output_table = f"{settings.catalog}.{settings.schema}.silver_customers"
    clean_bronze_data(spark, input_table, output_table)

if __name__ == "__main__":
    main()
