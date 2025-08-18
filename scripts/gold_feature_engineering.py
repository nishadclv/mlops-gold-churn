from src.data_engineering.gold_layer import engineer_features
from src.common.spark_utils import get_spark_session
from src.common.config import settings

def main():
    spark = get_spark_session()
    input_table = f"{settings.catalog}.{settings.schema}.silver_customers"
    output_table = f"{settings.catalog}.{settings.schema}.gold_churn_features"
    engineer_features(spark, input_table, output_table)

if __name__ == "__main__":
    main()
