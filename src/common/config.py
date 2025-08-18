# src/common/config.py - Simple version without pydantic
import os

class Settings:
    def __init__(self):
        self.catalog = os.getenv("UC_CATALOG", "ds_training_1")
        self.schema = os.getenv("UC_SCHEMA", "mlops_churn") 
        self.model_name = os.getenv("MODEL_NAME", "churn_prediction_model")
        self.experiment_path = os.getenv("EXPERIMENT_PATH", "/Shared/mlops-experiments")
        self.environment = os.getenv("ENVIRONMENT", "dev")

# Create the settings instance
settings = Settings()
