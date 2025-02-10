import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

spark = (SparkSession.builder
             .appName("etl_silver_socios_empresas")
              .config("spark.databricks.service.server.enabled", "true")
                .getOrCreate())

# Path source
path_bronze_soc = "dbfs:/FileStore/shared_uploads/default_user/bronze/*SOCIOCSV*.csv"

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

def extract_year_month(file_path):
    file_name = os.path.basename(file_path)
    year_month = file_name.split('_')[-1].split('.')[0]
    return year_month

# Adiciona colunas
df_list_soc = df_list_soc.withColumn("data_origem_arquivo", F.input_file_name())
df_list_soc = df_list_soc.withColumn("data_origem_arquivo", F.udf(extract_year_month, StringType())(col("data_origem_arquivo")))

# Data atual
current_date = datetime.now().strftime("%Y-%m-%d")

# Remoção de espaços em branco
df_list_soc = (df_list_soc
               .withColumn("nome_socio_format",
                           F.regexp_replace(col("nome_socio"), "^\\s+", ""))
                    .drop("nome_socio")
                        .withColumnRenamed("nome_socio_format", "nome_socio")
                        .withColumn("data_carga_dados", F.lit(current_date))
                        ).select(
                            'data_carga_dados'
                            , 'data_origem_arquivo'
                            , 'cnpj'
                            , 'tipo_socio'
                            , 'nome_socio'
                            , 'documento_socio'
                            , 'codigo_qualificacao_socio'
                        )
                        
# Criar schema no metastore
spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.db_rfb")
# Salvar tabela delta
df_list_soc.write.mode("append").format("delta").saveAsTable("hive_metastore.db_rfb.tbl_slv_socios")