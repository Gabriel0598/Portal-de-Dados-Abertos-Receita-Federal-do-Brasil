from pyspark.sql import SparkSession

def create_spark_session():
    spark = SparkSession.builder \
        .appName("etl_silver_socios_empresas") \
        .getOrCreate()
    return spark

create_spark_session()