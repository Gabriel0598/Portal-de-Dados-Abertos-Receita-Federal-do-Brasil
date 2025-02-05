import os
from pyspark.sql import SparkSession

# Unset SPARK_HOME environment variable
# if 'SPARK_HOME' in os.environ:
#    del os.environ['SPARK_HOME']

spark = (SparkSession.builder
             .appName("etl_silver_socios_empresas")
              .config("spark.databricks.service.server.enabled", "true")
                .getOrCreate())

bronze_path_empre = os.path.join(os.getcwd(), 'data/bronze/land_zone/*EMPRECSV')

# df = spark.read.csv(bronze_path_empre, header=True, inferSchema=True)
# df.printSchema()