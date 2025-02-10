import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, current_date, when

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

# Join entre tabela empresa e socios/ apenas registros com correspondencia completa
df_join_emp_soc = (df_slv_emp
                    .join(df_slv_soc
                          , df_slv_emp.cnpj_emp == df_slv_soc.cnpj_soc
                          , 'inner')
                    )

df_join_emp_soc.printSchema()

# Consulta quantidade de sócios por empresas num período de três meses
df_flag_soc = (df_join_emp_soc
                  .select("data_carga_dados_soc", "data_origem_arquivo_soc"
                          , "cnpj_soc", "cod_porte", "nome_socio", "documento_socio")
                    .filter(col("data_origem_arquivo_soc") >= F.date_sub(current_date(), 90))
                      .groupBy("cnpj_soc", "documento_socio", "nome_socio", "cod_porte")
                        .agg(count("nome_socio").alias("qtde_socios"))
                          .withColumn("flag_socio_estrangeiro"
                                , when(col("documento_socio") == "***999999**", True)
                                .otherwise(False))
                            .withColumn("doc_alvo"
                                    , when(((col("cod_porte") == 3) & (col("qtde_socios") >= 1)), True)
                                    .otherwise(False))
                            )

df_dates_emp = df_join_emp_soc.select("data_carga_dados_emp", "data_origem_arquivo_emp", "cnpj_emp")

df_gold_final = (df_flag_soc
                 .join(
                    df_dates_emp
                    , df_flag_soc.cnpj_soc == df_dates_emp.cnpj_emp
                    , 'inner'
                 ).withColumnRenamed("cnpj_emp", "cnpj")
                 .withColumnRenamed("data_carga_dados_emp", "data_carga_dados")
                 .withColumnRenamed("data_origem_arquivo_emp", "data_origem_arquivo")
                 .select(
                      'data_carga_dados'
                      , 'data_origem_arquivo'
                      , 'cnpj'
                      , 'qtde_socios'
                      , 'flag_socio_estrangeiro'
                      , 'doc_alvo'
                 ))

# Criar schema no metastore
spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.db_rfb")
# Salvar tabela delta
df_gold_final.write.mode("append").format("delta").saveAsTable("hive_metastore.db_rfb.tbl_gld_socios_doc_alvo")