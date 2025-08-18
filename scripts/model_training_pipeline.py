from src.ml_engineering.model_training import train_model
from src.common.spark_utils import get_spark_session
from src.common.config import settings

def main():
    spark = get_spark_session()
    gold_table = f"{settings.catalog}.{settings.schema}.gold_churn_features"
    metrics = train_model(spark, gold_table)
    print("Model metrics:", metrics)

if __name__ == "__main__":
    main()
