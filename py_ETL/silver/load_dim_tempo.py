from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, floor, year, month, dayofmonth, dayofweek, date_format, concat
from pyspark.sql.types import IntegerType, StringType
from datetime import datetime, timedelta
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

# Criar a sessão Spark
spark = SparkSession.builder \
    .appName("Populando Tabela Dimensão Tempo") \
    .getOrCreate()

destinationParquet = config.LAKE_SILVER_PATH + "dim_tempo/"

# Função para gerar as datas entre 01/01/1995 e 31/12/2015
def generate_date_range(start_date, end_date):
    current_date = start_date
    date_list = []
    
    while current_date <= end_date:
        date_list.append(current_date)
        current_date += timedelta(days=1)
    
    return date_list

# Definindo o range de datas
start_date = datetime(1995, 1, 1)
end_date = datetime(2015, 12, 31)
dates = generate_date_range(start_date, end_date)

# Criar um DataFrame de datas
df = spark.createDataFrame([(d,) for d in dates], ["data"])

# Adicionando colunas para a dimensão tempo
df = df.withColumn("dim_tempo_id", concat(year(col("data")).cast(StringType()), \
                                          month(col("data")).cast(StringType()), \
                                          dayofmonth(col("data")).cast(StringType()))) \
    .withColumn("mes", month(col("data")).cast(StringType())) \
    .withColumn("ano", year(col("data")).cast(StringType())) \
    .withColumn("dia_mes", dayofmonth(col("data")).cast(StringType())) \
    .withColumn("dia_semana_num", dayofweek(col("data")).cast(StringType())) \
    .withColumn("trimestre", (floor((month(col("data")) - 1) / 3) + 1).cast(StringType())) \
    .withColumn("dia_semana_abrev", date_format(col("data"), "E").cast(StringType())) \
    .withColumn("dia_semana", date_format(col("data"), "EEEE").cast(StringType())) \
    .withColumn("mes_descricao_abrev", date_format(col("data"), "MMM").cast(StringType())) \
    .withColumn("mes_descricao", date_format(col("data"), "MMMM").cast(StringType())) \
    .withColumn("semestre", (floor((month(col("data")) - 1) / 6) + 1).cast(StringType()))


# Mostrar resultado
df.show()

# Salvar o DataFrame como tabela (substitua o caminho pelo seu destino desejado)
df.write.mode("overwrite").format("parquet").save(destinationParquet)

# Encerrar a sessão Spark
spark.stop()
