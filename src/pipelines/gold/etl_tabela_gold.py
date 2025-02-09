import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

# Unset SPARK_HOME environment variable
# if 'SPARK_HOME' in os.environ:
#    del os.environ['SPARK_HOME']

spark = (SparkSession.builder
             .appName("etl_silver_socios_empresas")
              .config("spark.databricks.service.server.enabled", "true")
                .getOrCreate())

# Referencia de tabelas
tbl_empresas = 'hive_metastore.db_rfb.tbl_slv_empresas'
tbl_socios = 'hive_metastore.db_rfb.tbl_slv_socios'

df_slv_emp = (spark.read.table(tbl_empresas)
              .withColumnRenamed("cnpj", "cnpj_emp")
                .withColumnRenamed("data_carga_dados", "data_carga_dados_emp")
                    .withColumnRenamed("data_origem_arquivo", "data_origem_arquivo_emp")
)

df_slv_soc = (spark.read.table(tbl_socios)
              .withColumnRenamed("cnpj", "cnpj_soc")
                .withColumnRenamed("data_carga_dados", "data_carga_dados_soc")
                    .withColumnRenamed("data_origem_arquivo", "data_origem_arquivo_soc")
)

df_slv_emp.printSchema()
df_slv_soc.printSchema()

df_join_emp_soc = (df_slv_emp
                    .join(df_slv_soc
                          , df_slv_emp.cnpj_emp == df_slv_soc.cnpj_soc
                          , 'inner')
                    )

df_join_emp_soc.printSchema()