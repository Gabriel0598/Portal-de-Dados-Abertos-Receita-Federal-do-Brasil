import os
from pyspark.sql import SparkSession

# Unset SPARK_HOME environment variable
# if 'SPARK_HOME' in os.environ:
#    del os.environ['SPARK_HOME']

spark = (SparkSession.builder
             .appName("etl_silver_socios_empresas")
              .config("spark.databricks.service.server.enabled", "true")
                .getOrCreate())

bronze_path_empre = "dbfs:/FileStore/shared_uploads/default_user/bronze/K3241.K03200Y1.D50111.EMPRECSV"

df = spark.read.options(header=True, inferSchema=True, sep=';').format("csv")
#.csv(bronze_path_empre, header=True, inferSchema=True)
df.printSchema()