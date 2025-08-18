from databricks.feature_store import FeatureStoreClient
from src.common.logging import get_structured_logger
from src.common.config import settings

logger = get_structured_logger("feature_store")

def register_features(spark, features_df, table_name, primary_key):
    fs = FeatureStoreClient()
    full_table = f"{settings.catalog}.{settings.schema}.{table_name}"
    fs.create_table(
        name=full_table,
        primary_keys=[primary_key],
        df=features_df,
        description="Gold churn features"
    )
    logger.info("Feature table registered", table=full_table)
