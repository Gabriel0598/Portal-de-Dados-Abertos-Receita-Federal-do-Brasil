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

# Path source
path_bronze_empre = "dbfs:/FileStore/shared_uploads/default_user/bronze/*EMPRECSV"

# definicao de schema
schemaEmpresas = StructType([
    StructField("cnpj", IntegerType(), True),
    StructField("razao_social", StringType(), True),
    StructField("natureza_juridica", IntegerType(), True),
    StructField("qualificao_responsavel", IntegerType(), True),
    StructField("capital_social", FloatType(), True),
    StructField("cod_porte", StringType(), True),
    StructField("localizacao", StringType(), True)
])

# Leitura de arquivo bruto
df_list_emp = spark.read.options(header=False, inferSchema=True, sep=';') \
                .format("csv") \
                    .schema(schemaEmpresas) \
                        .load(path_bronze_empre)

# Remoção de espaços em branco                       
df_list_emp = (df_list_emp
               .withColumn("razao_social_format",
                           F.regexp_replace(col("razao_social"), "^\\s+", ""))
                    .drop("razao_social")
                        .withColumnRenamed("razao_social_format", "razao_social")
                        ).select(
                            'cnpj'
                            , 'razao_social'
                            , 'natureza_juridica'
                            , 'qualificao_responsavel'
                            , 'capital_social'
                            , 'cod_porte'
                        )

# Criar schema no metastore
spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.db_rfb")
# Salvar tabela delta
df_list_emp.write.mode("append").format("delta").saveAsTable("hive_metastore.db_rfb.tbl_slv_empresas")