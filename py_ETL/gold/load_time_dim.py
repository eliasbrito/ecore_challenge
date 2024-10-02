from pyspark.sql import SparkSession
import mysql.connector
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

# Criar a sessão Spark
spark = SparkSession.builder \
    .appName("Carga Time Dimension") \
    .getOrCreate()

sourceParquet = config.LAKE_SILVER_PATH + "dim_tempo/"

# Lê o arquivo Parquet
time_dim_df = spark.read.parquet(sourceParquet)

# Configurações de conexão MySQL
def get_mysql_connection():
    conn = mysql.connector.connect(
        host=config.host,
        user=config.usuario,
        password=password,
        database=config.database
    )
    return conn

# Grava os dados na tabela time_dim
def load_time_dimension(dataframe):
    # Iterar sobre as linhas do DataFrame
    for row in dataframe.collect():
        # Extrai os valores da linha
        data = row['data']
        mes = row['mes']
        ano = row['ano']
        dia_mes = row['dia_mes']
        dia_semana_num = row['dia_semana_num']
        trimestre = row['trimestre']
        dia_semana_abrev = row['dia_semana_abrev']
        dia_semana = row['dia_semana']
        mes_descricao_abrev = row['mes_descricao_abrev']
        mes_descricao = row['mes_descricao']
        semestre = row['semestre']

        # Insere na tabela time_dim
        conn = get_mysql_connection()
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO time_dim 
            (data, mes, ano, dia_mes, dia_semana_num, trimestre, 
            dia_semana_abrev, dia_semana, mes_descricao_abrev, 
            mes_descricao, semestre) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (data, mes, ano, dia_mes, dia_semana_num, trimestre,
                                       dia_semana_abrev, dia_semana, mes_descricao_abrev,
                                       mes_descricao, semestre))
        conn.commit()
        cursor.close()
        conn.close()

# Carregar os dados
load_time_dimension(time_dim_df)

# Encerra a sessão Spark
spark.stop()
