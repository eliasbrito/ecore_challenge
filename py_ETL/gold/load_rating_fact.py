from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
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

# Inicializa a sessão do Spark
spark = SparkSession.builder \
    .appName("Load Rating Fact") \
    .config("spark.executor.memory", config.MAX_MEMORY) \
    .config("spark.executor.cores", config.EXECUTOR_CORES) \
    .config("spark.driver.memory", config.DRIVER_MEMORY) \
    .config("spark.driver.extraClassPath", config.JDBC_PATH) \
    .getOrCreate()

sourceParquet = config.LAKE_SILVER_PATH + "rating/"

# Carrega os arquivos parquet
rating_df = spark.read.parquet(sourceParquet)

# Carrega as tabelas product_dim e time_dim via JDBC
product_dim_df = spark.read.format("jdbc") \
    .option("url", config.url) \
    .option("dbtable", "product_dim") \
    .option("user", config.usuario) \
    .option("password", password) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

time_dim_df = spark.read.format("jdbc") \
    .option("url", config.url) \
    .option("dbtable", "time_dim") \
    .option("user", config.usuario) \
    .option("password", password) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

# Função para aplicar as regras de transformação
def transform_and_filter_ratings(df: DataFrame, company: str, limit: int) -> DataFrame:
    # Filtro por empresa
    filtered_df = df.filter(col("company") == company)
    
    # Join com product_dim para buscar a surrogate key do produto
    product_joined_df = filtered_df.join(product_dim_df, 
                                         (filtered_df["product_id"] == product_dim_df["product_id"]) &
                                         (filtered_df["company"] == product_dim_df["company"]),
                                         how="inner") \
                                   .select(filtered_df["*"], product_dim_df["surrogate_key"].alias("product_surrogate_key"))

    # Join com time_dim para buscar a surrogate key da data
    time_joined_df = product_joined_df.join(time_dim_df, 
                                            product_joined_df["review_date"] == time_dim_df["data"],
                                            how="inner") \
                                      .select(product_joined_df["*"], time_dim_df["surrogate_key"].alias("time_surrogate_key"))
    
    # Seleciona apenas as colunas necessárias e aplica o limite de 5000 registros
    final_df = time_joined_df.select("product_surrogate_key", "time_surrogate_key", "customer_id", "star_rating") \
                             .limit(limit)
    
    return final_df

# Aplica a transformação e filtra 5000 registros para Amazon e Netflix
amazon_ratings = transform_and_filter_ratings(rating_df, "Amazon", 5000)
netflix_ratings = transform_and_filter_ratings(rating_df, "Netflix", 5000)

# Combina os dados
final_ratings_df = amazon_ratings.union(netflix_ratings)

final_ratings_df.show()
# Escreve os dados no MySQL
final_ratings_df.write.format("jdbc") \
    .option("url", config.url) \
    .option("dbtable", "rating_fact") \
    .option("user", config.usuario) \
    .option("password", password) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .mode("append") \
    .save()

# Encerra a sessão do Spark
spark.stop()
