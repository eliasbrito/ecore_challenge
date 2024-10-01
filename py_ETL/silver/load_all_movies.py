from pyspark.sql import SparkSession 
from pyspark.sql.functions import lit, regexp_replace #function para criar coluna de valor constante no meu df, function para trabalhar com regex
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

pathParquetFilesNetflix = config.LAKE_BRONZE_PATH + "movies_netflix"
pathParquetFilesAmazon = config.LAKE_BRONZE_PATH + "rating_amazon"
destinationParquet = config.LAKE_SILVER_PATH + "movies/"

# Criar a sessão Spark
spark = SparkSession.builder \
    .appName("Movies Parquet") \
    .getOrCreate()

#Netflix
df_movies_netflix = spark.read.parquet(pathParquetFilesNetflix)
df_movies_netflix = df_movies_netflix.withColumnRenamed("year", "year_released")
df_movies_netflix = df_movies_netflix.withColumn("company", lit("Netflix"))

#Amazon
df_movies_amazon = spark.read.parquet(pathParquetFilesAmazon)
df_movies_amazon = df_movies_amazon.select("product_id", 
    lit(0).alias("year_released"),
    regexp_replace(df_movies_amazon.product_title, " \[.*", "").alias("title"), #remove coisas como " [VHS]", " [DVD]"
    lit("Amazon").alias("company "))
df_movies_amazon = df_movies_amazon.distinct()

df_movies = df_movies_netflix.union(df_movies_amazon)

df_movies.write.mode("overwrite").parquet(destinationParquet)

# Mostrar o DataFrame final
#df_movies.show()

spark.stop()