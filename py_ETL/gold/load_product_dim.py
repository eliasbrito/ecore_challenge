from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import sys
from cryptography.fernet import Fernet
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

# Descriptografa a senha
key = config.KEY.encode()
cipher_suite = Fernet(key)
decrypted_password = cipher_suite.decrypt(config.ENCRYPTED_PASSWORD.encode())

# Converte a senha de volta para string
password = decrypted_password.decode()

# Criar a sessão do Spark
spark = SparkSession.builder \
    .appName("Carga de Dados Parquet para MySQL") \
    .config("spark.driver.extraClassPath", config.JDBC_PATH) \
    .getOrCreate()

# Configurações da conexão com o MySQL
url = config.url
tabela_mysql = "product_dim"
usuario = config.usuario
senha = password

# Carregar o arquivo Parquet
parquet_df = spark.read.parquet(config.LAKE_SILVER_PATH + "movies/")

# Filtrar 1000 linhas para a company 'Netflix'
netflix_df = parquet_df.filter(parquet_df.company == 'Netflix').limit(1000)

# Filtrar 1000 linhas para a company 'Amazon'
amazon_df = parquet_df.filter(parquet_df.company == 'Amazon').limit(1000)

# Unir os DataFrames filtrados
final_df = netflix_df.union(amazon_df)

# Ler dados da tabela MySQL
existing_data_df = spark.read.format("jdbc").option("url", url) \
    .option("dbtable", tabela_mysql) \
    .option("user", usuario) \
    .option("password", senha) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

# Realizar o join entre os dados novos e os existentes
joined_df = final_df.alias("new").join(
    existing_data_df.alias("existing"),
    (col("new.product_id") == col("existing.product_id")) &
    (col("new.company") == col("existing.company")),
    "outer"
)

# Identificar registros que precisam ser inseridos
to_insert_df = joined_df.filter(col("existing.product_id").isNull()).select(
    col("new.product_id").alias("product_id"),
    col("new.year_released").alias("year_released"),
    col("new.title").alias("title"),
    col("new.company").alias("company")
)

# Identificar registros que precisam ser atualizados
to_update_df = joined_df.filter(col("existing.product_id").isNotNull() & (
    (col("new.title") != col("existing.title")) | (col("new.year_released") != col("existing.year_released"))
)).select(
    col("existing.product_id").alias("product_id"),
    col("new.title").alias("title"),
    col("new.year_released").alias("year_released"),
    col("new.company").alias("company")
)

# Escrever os dados que precisam ser inseridos no MySQL
to_insert_df.write.format("jdbc") \
    .option("url", config.url) \
    .option("dbtable", tabela_mysql) \
    .option("user", config.usuario) \
    .option("password", password) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .mode("append") \
    .save()

# Atualizar registros na tabela MySQL
# Para a atualização, vamos usar o método de execução de comandos SQL
# Para isso, será necessário importar o módulo para criar a conexão com o MySQL
import mysql.connector

# Conexão com o MySQL
conn = mysql.connector.connect(
    host=config.host,
    user=config.usuario,
    password=password,
    database=config.database
)

cursor = conn.cursor()

# Atualizar registros
for row in to_update_df.collect():
    update_query = f"""
    UPDATE {tabela_mysql} 
    SET title = '{row.title}', year_released = '{row.year_released}' 
    WHERE product_id = {row.product_id} AND company = '{row.company}'
    """
    cursor.execute(update_query)

# Commit as atualizações
conn.commit()

# Fechar a conexão
cursor.close()
conn.close()

# Parar a sessão do Spark
spark.stop()
