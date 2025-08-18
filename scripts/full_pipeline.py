from scripts.bronze_ingestion import main as bronze_main
from scripts.silver_transformation import main as silver_main
from scripts.gold_feature_engineering import main as gold_main
from scripts.model_training_pipeline import main as train_main

def run_full_pipeline():
    bronze_main()
    silver_main()
    gold_main()
    train_main()

if __name__ == "__main__":
    run_full_pipeline()
