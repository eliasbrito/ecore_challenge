from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
import glob
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

# Criar a sessão Spark
spark = SparkSession.builder \
    .appName("Extrai arquivo de rating Netflix") \
    .config("spark.executor.memory", config.MAX_MEMORY) \
    .config("spark.executor.cores", config.EXECUTOR_CORES) \
    .config("spark.driver.memory", config.DRIVER_MEMORY) \
    .getOrCreate()

# Definir o padrão para encontrar os arquivos com sufixo _n
sourceFilePattern = config.LAKE_STG_PATH +  "netflix/combined_data_*.txt"
destinationParquet = config.LAKE_BRONZE_PATH + "bronze/rating_netflix/"

# Usar glob para encontrar todos os arquivos que correspondem ao padrão
files = glob.glob(sourceFilePattern)

# Ordenar os arquivos para garantir que sejam lidos na ordem correta
files.sort()

# Definir o esquema para o DataFrame
schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("star_rating", StringType(), True),
    StructField("review_date", StringType(), True)
])




# Inicializar um DataFrame vazio com o esquema definido
df_final = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

# Função para processar as linhas do arquivo
def processar_particao(particao):
    filme_atual = None
    dados_filmes = []
    
    for linha in particao:
        linha = linha.strip()
        if ":" in linha:
            # Se a linha contém o código do filme
            filme_atual = linha.split(":")[0].strip()  # Remover espaços em branco
        elif linha and filme_atual:  # Verifique se filme_atual não é None
            # Se a linha contém os dados (id_cliente, nota, data)
            customer_id, star_rating, review_date = linha.split(",")
            dados_filmes.append((filme_atual, customer_id.strip(), star_rating.strip(), review_date.strip()))
    
    return dados_filmes

# Ler e processar cada arquivo, unindo os DataFrames
for arquivo in files:
    rdd = spark.sparkContext.textFile(arquivo)
    
    # Aplicar a função a cada partição do RDD
    rdd_dados = rdd.mapPartitions(processar_particao)

    # Converter o RDD em um DataFrame
    df_temp = rdd_dados.map(lambda x: Row(product_id=x[0], customer_id=x[1], star_rating=x[2], review_date=x[3])).toDF(schema)

    # Combinar os dados no DataFrame final
    df_final = df_final.union(df_temp)

# Ordenar o DataFrame pelo id_movie (ou pela coluna desejada)
df_final = df_final.orderBy("product_id")  # Ajuste conforme necessário

# Mostrar o DataFrame final
df_final.show()

# Escrever o DataFrame em um arquivo Parquet
df_final.write.mode("overwrite").parquet(destinationParquet)

# Encerrar a sessão Spark
spark.stop()
