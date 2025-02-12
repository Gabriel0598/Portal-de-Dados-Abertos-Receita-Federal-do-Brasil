import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.functions import col, udf

spark = (SparkSession.builder
             .appName("etl_silver_socios_empresas")
              .config("spark.databricks.service.server.enabled", "true")
                .getOrCreate())

# Path source
path_bronze_empre = "dbfs:/FileStore/shared_uploads/default_user/bronze/*EMPRECSV*.csv"

# definicao de schema
schemaEmpresas = StructType([
    StructField("cnpj", IntegerType(), True),
    StructField("razao_social", StringType(), True),
    StructField("natureza_juridica", IntegerType(), True),
    StructField("qualificao_responsavel", IntegerType(), True),
    StructField("capital_social", DecimalType(), True),
    StructField("cod_porte", IntegerType(), True),
    StructField("localizacao", StringType(), True)
])

# Leitura de arquivo bruto
df_list_emp = spark.read.options(header=False, inferSchema=True, sep=';') \
                .format("csv") \
                    .schema(schemaEmpresas) \
                        .load(path_bronze_empre)

# Extração do ano e mês do nome do arquivo
def extract_year_month(file_path):
    file_name = os.path.basename(file_path)
    year_month = file_name.split('_')[-1].split('.')[0]
    return year_month

# Register UDF
extract_year_month_udf = udf(extract_year_month, StringType())

# Adiciona colunas
df_list_emp = df_list_emp.withColumn("data_origem_arquivo", F.input_file_name())
df_list_emp = df_list_emp.withColumn("data_origem_arquivo", extract_year_month_udf(col("data_origem_arquivo")))
df_list_emp = df_list_emp.withColumn("data_origem_arquivo", F.to_date(col("data_origem_arquivo"), "yyyy-MM"))

# Data atual
current_date = datetime.now().strftime("%Y-%m-%d")

# Remoção de espaços em branco
df_list_emp = (df_list_emp
               .withColumn("razao_social_format",
                           F.regexp_replace(col("razao_social"), "^\\s+", ""))
                    .drop("razao_social")
                        .withColumnRenamed("razao_social_format", "razao_social")
                        .withColumn("data_carga_dados", F.lit(current_date))
                        .withColumn("data_carga_dados", F.to_date(col("data_carga_dados")))
                        ).select(
                            'data_carga_dados'
                            , 'data_origem_arquivo'
                            , 'cnpj'
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