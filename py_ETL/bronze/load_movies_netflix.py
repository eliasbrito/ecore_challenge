from pyspark.sql import SparkSession 
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

# Criar a sessão Spark
spark = SparkSession.builder \
    .appName("Leitura arquivo estruturado") \
    .getOrCreate()

sourceFilePath = config.LAKE_STG_PATH + "netflix/movie_titles.csv"
destinationParquet = config.LAKE_BRONZE_PATH + "/movies_netflix/"

df = spark.read.csv(sourceFilePath, header=False, sep=",",  inferSchema=True)
# Nomear as colunas manualmente
colunas = ["product_id", "year", "title"]  # Adapte ao número de colunas do seu CSV
df = df.toDF(*colunas)

# Mostrar o DataFrame final
#df.show()

df.write.mode("overwrite").parquet(destinationParquet)
# Encerrar a sessão Spark
spark.stop()