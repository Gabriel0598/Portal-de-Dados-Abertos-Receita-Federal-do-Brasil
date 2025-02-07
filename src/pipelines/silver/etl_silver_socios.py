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
path_bronze_soc = "dbfs:/FileStore/shared_uploads/default_user/bronze/*SOCIOCSV"

# definicao de schema
schemaSocios = StructType([
    StructField("cnpj", IntegerType(), True),
    StructField("tipo_socio", IntegerType(), True),
    StructField("nome_socio", StringType(), True),
    StructField("documento_socio", StringType(), True),
    StructField("codigo_qualificacao_socio", IntegerType(), True),
    StructField("cnpj_seg_num", IntegerType(), True),
    StructField("documento_co_responsavel", StringType(), True),
    StructField("nome_co_responsavel", StringType(), True),
    StructField("codigo_qualificacao_co_responsavel", IntegerType(), True),
    StructField("id_registro_co_responsavel", IntegerType(), True)
])

# Leitura de arquivo bruto
df_list_soc = spark.read.options(header=False, inferSchema=True, sep=';') \
                .format("csv") \
                    .schema(schemaSocios) \
                        .load(path_bronze_soc)
                        
# Remoção de espaços em branco                       
df_list_soc = (df_list_soc
               .withColumn("nome_socio_format",
                           F.regexp_replace(col("nome_socio"), "^\\s+", ""))
                    .drop("nome_socio")
                        .withColumnRenamed("nome_socio_format", "nome_socio")
                        ).select(
                            'cnpj'
                            , 'tipo_socio'
                            , 'nome_socio'
                            , 'documento_socio'
                            , 'codigo_qualificacao_socio'
                        )
                        
# Criar schema no metastore
spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.db_rfb")
# Salvar tabela delta
df_list_soc.write.mode("append").format("delta").saveAsTable("hive_metastore.db_rfb.tbl_slv_socios")