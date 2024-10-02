from pyspark.sql import SparkSession 
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

# Criar a sessão Spark
spark = SparkSession.builder \
    .appName("Leitura arquivo estruturado") \
    .config("spark.executor.memory", config.MAX_MEMORY) \
    .config("spark.executor.cores", config.EXECUTOR_CORES) \
    .config("spark.driver.memory", config.DRIVER_MEMORY) \
    .getOrCreate()

sourceFilePath = config.LAKE_STG_PATH + "amazon/amazon_reviews_us_*.tsv"
destinationParquet = config.LAKE_BRONZE_PATH + "/rating_amazon/"

df = spark.read.csv(sourceFilePath, header=True, sep="\t",  inferSchema=True)
# Mostrar o DataFrame final
#df.show()

df.write.mode("overwrite").parquet(destinationParquet)
# Encerrar a sessão Spark
spark.stop()