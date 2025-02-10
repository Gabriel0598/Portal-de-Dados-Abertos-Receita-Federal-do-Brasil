import os
import mysql.connector
from mysql.connector import errorcode
from pyspark.sql import SparkSession

# Configurações do MySQL usando variáveis de ambiente
config = {
    'user': os.getenv('MYSQL_USER', 'myuser'),
    'password': os.getenv('MYSQL_PASSWORD', 'mypassword'),
    'host': '127.0.0.1',
    'database': os.getenv('MYSQL_DATABASE', 'mydatabase'),
    'raise_on_warnings': True
}


# Função para criar tabelas se não existirem
def create_tables(cursor):
    tables = {
        'tbl_slv_empresas': (
            "CREATE TABLE IF NOT EXISTS tbl_slv_empresas ("
            "  data_carga_dados DATE"
            "  , data_origem_arquivo DATE"
            "  , cnpj BIGINT"
            "  , razao_social VARCHAR(255)"
            "  , natureza_juridica INT"
            "  , qualificao_responsavel INT"
            "  , capital_social DECIMAL(15, 2)"
            "  , cod_porte INT"
            ")"
        ),
         'tbl_slv_socios': (
            "CREATE TABLE IF NOT EXISTS tbl_slv_socios ("
            "  data_carga_dados DATE"
            "  , data_origem_arquivo DATE"
            "  , cnpj BIGINT"
            "  , tipo_socio INT"
            "  , nome_socio VARCHAR(255)"
            "  , documento_socio VARCHAR(255)"
            "  , codigo_qualificacao_socio INT"
            ")"
        ),
        'tbl_gld_socios_doc_alvo': (
            "CREATE TABLE IF NOT EXISTS tbl_gld_socios_doc_alvo ("
            "  data_carga_dados DATE"
            "  , data_origem_arquivo DATE"
            "  , cnpj BIGINT"
            "  , qtde_socios INT"
            "  , flag_socio_estrangeiro BOOLEAN"
            "  , doc_alvo BOOLEAN"
            ")"
        )
    }

    for table_name, table_description in tables.items():
        try:
            print(f"Creating table {table_name}: ", end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

# Função para carregar dados do Spark DataFrame para MySQL
def load_data_to_mysql(df, table_name):
    df.write \
      .format("jdbc") \
      .option("url", f"jdbc:mysql://127.0.0.1:3306/{os.getenv('MYSQL_DATABASE', 'mydatabase')}") \
      .option("driver", "com.mysql.cj.jdbc.Driver") \
      .option("dbtable", table_name) \
      .option("user", os.getenv('MYSQL_USER', 'myuser')) \
      .option("password", os.getenv('MYSQL_PASSWORD', 'mypassword')) \
      .mode("append") \
      .save()

# Inicializa Spark
spark = SparkSession.builder \
    .appName("LoadDataToMySQL") \
    .getOrCreate()

# Carrega dados das tabelas Silver e Gold
df_slv_emp = spark.table("hive_metastore.db_rfb.tbl_slv_empresas")
df_slv_soc = spark.table("hive_metastore.db_rfb.tbl_slv_socios")
df_gld_soc_emp = spark.table("hive_metastore.db_rfb.tbl_gld_socios_doc_alvo")

# Conecta ao MySQL e cria tabelas se não existirem
try:
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    create_tables(cursor)
    cnx.commit()
except mysql.connector.Error as err:
    print(err)
finally:
    cursor.close()
    cnx.close()

# Carrega dados para MySQL
load_data_to_mysql(df_slv_emp, "tbl_slv_empresas")
load_data_to_mysql(df_slv_soc, "tbl_slv_socios")
load_data_to_mysql(df_gld_soc_emp, "tbl_gld_socios_doc_alvo")