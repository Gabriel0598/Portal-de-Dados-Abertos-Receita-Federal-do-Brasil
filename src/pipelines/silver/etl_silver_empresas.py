import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col

# Unset SPARK_HOME environment variable
# if 'SPARK_HOME' in os.environ:
#    del os.environ['SPARK_HOME']

spark = (SparkSession.builder
             .appName("etl_silver_socios_empresas")
              .config("spark.databricks.service.server.enabled", "true")
                .getOrCreate())

path_bronze_empre = "dbfs:/FileStore/shared_uploads/default_user/bronze/*EMPRECSV"

# definicao de schema
schemaEmpresas = StructType([
    StructField("id", IntegerType(), True),
    StructField("razao_social", StringType(), True),
    StructField("cod_natureza_juridica", IntegerType(), True),
    StructField("cod_qualificao_responsavel", IntegerType(), True),
    StructField("capital_social", FloatType(), True),
    StructField("cod_porte", StringType(), True),
    StructField("localizacao", StringType(), True)
])

df_list_emp = spark.read.options(header=False, inferSchema=True, sep=';') \
                .format("csv") \
                    .schema(schemaEmpresas) \
                        .load(path_bronze_empre)
                        
df_list_emp = (df_list_emp
               .withColumn("razao_social_format",
                           F.regexp_replace(col("razao_social"), "^\\s+", ""))
                    .drop("razao_social")
                        .withColumnRenamed("razao_social_format", "razao_social")
                        ).select(
                            'id'
                            , 'razao_social'
                            , 'cod_natureza_juridica'
                            , 'cod_qualificao_responsavel'
                            , 'capital_social'
                            , 'cod_porte'
                            , 'localizacao'
                        )

# Criar schema no metastore
spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.db_rfb")
# Salvar tabela delta
df_list_emp.write.mode("append").format("delta").saveAsTable("hive_metastore.db_rfb.tbl_slv_empresas")