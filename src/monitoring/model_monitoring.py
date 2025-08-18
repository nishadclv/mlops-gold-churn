from src.common.logging import get_structured_logger

logger = get_structured_logger("model_monitoring")

def log_model_performance(metrics):
    logger.info("Model performance log", metrics=metrics)
    # Use Databricks SQL/MLflow for dashboards
    return metrics
