from pyspark.sql import SparkSession 
from pyspark.sql.functions import lit, regexp_replace #function para criar coluna de valor constante no meu df, function para trabalhar com regex
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

pathParquetFilesNetflix = config.LAKE_BRONZE_PATH + "rating_netflix"
pathParquetFilesAmazon = config.LAKE_BRONZE_PATH + "rating_amazon"
destinationParquet = config.LAKE_SILVER_PATH + "rating/"

# Criar a sessão Spark
spark = SparkSession.builder \
    .appName("Ratings Parquet") \
    .getOrCreate()

#Netflix
df_rating_netflix = spark.read.parquet(pathParquetFilesNetflix)
df_rating_netflix = df_rating_netflix.withColumn("company", lit("Netflix"))

#Amazon
df_rating_amazon = spark.read.parquet(pathParquetFilesAmazon)
df_rating_amazon = df_rating_amazon.select("product_id", "customer_id", "star_rating", "review_date",lit("Amazon").alias("company"))

df_rating = df_rating_amazon.union(df_rating_netflix)

df_rating.write.mode("overwrite").parquet(destinationParquet)

df_rating.show()

spark.stop()