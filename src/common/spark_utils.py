from pyspark.sql import SparkSession

def get_spark_session():
    """Gets or creates Spark session – works both on Databricks and locally."""
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
