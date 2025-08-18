import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
import mlflow
from src.common.config import settings
from src.common.logging import get_structured_logger

logger = get_structured_logger("model_training")

def train_model(spark, gold_table):
    df = spark.table(gold_table).toPandas().dropna()
    X = pd.get_dummies(
        df.drop(columns=["churn_label", "customer_id", "churn"]), drop_first=True
    )
    y = df["churn_label"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]
    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "f1_score": f1_score(y_test, y_pred),
        "roc_auc": roc_auc_score(y_test, y_proba),
    }
    mlflow.set_experiment(settings.experiment_path)
    with mlflow.start_run():
        mlflow.log_params({"n_estimators": 100})
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(model, artifact_path="model", registered_model_name=settings.model_name)
    logger.info("Model training complete", metrics=metrics)
    return metrics
