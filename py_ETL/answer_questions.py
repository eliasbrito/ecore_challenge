from pyspark.sql import SparkSession
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

# Criar a SparkSession
spark = SparkSession.builder \
    .appName("Respondendo as perguntas do challenge usando SQL") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Ler os arquivos Parquet em DataFrames
df_filmes = spark.read.parquet(config.LAKE_SILVER_PATH + "movies")
df_avaliacoes = spark.read.parquet(config.LAKE_SILVER_PATH + "rating")
#df_avaliacoes.show()
# Registrar os DataFrames como tabelas temporárias
df_filmes.createOrReplaceTempView("filmes")
df_avaliacoes.createOrReplaceTempView("avaliacoes")

# Question 1 - Quantos filmes estão disponíveis na Amazon? E na Netflix?
query = """
SELECT 
    count(*) qtd_filmes_disponiveis, company 
FROM 
    filmes f
group by company
"""

# Executar a consulta SQL
df_resultado = spark.sql(query)

print("Question 1 - Quantos filmes estão disponíveis na Amazon? E na Netflix?")

# Exibir o resultado do JOIN
df_resultado.show()

# Question 2 - Dos filmes disponíveis na Amazon, quantos % estão disponíveis na Netflix?
query = """
SELECT 
        COUNT(distinct CASE WHEN f_netflix.title IS NOT NULL THEN f_amazon.title END) as qtd_netflix,
        COUNT(distinct f_amazon.title) as qtd_amazon,
        round(COUNT(distinct CASE WHEN f_netflix.title IS NOT NULL THEN f_amazon.title END) /COUNT(distinct f_amazon.title) * 100,2) as perc_amazon_netflix        
FROM 
    (SELECT title FROM filmes WHERE company = 'Amazon') f_amazon
LEFT JOIN 
    (SELECT title FROM filmes WHERE company = 'Netflix') f_netflix
ON f_amazon.title = f_netflix.title;

"""
# Executar a consulta SQL
df_resultado = spark.sql(query)

print("Question 2 - Dos filmes disponíveis na Amazon, quantos por cento estão disponíveis na Netflix?")
# Exibir o resultado do JOIN
df_resultado.show()

# Question 3 - O quão perto a média das notas dos filmes disponíveis na Amazon está dos filmes disponíveis na Netflix?

query = """
SELECT 
    round(AVG(CASE WHEN company = 'Amazon' THEN star_rating END),2) as media_amazon,
    round(AVG(CASE WHEN company = 'Netflix' THEN star_rating END),2) as media_netflix,
    round(AVG(CASE WHEN company = 'Amazon' THEN star_rating END) / AVG(CASE WHEN company = 'Netflix' THEN star_rating END),2) AS proporcao_amazon_netflix
FROM 
    avaliacoes
WHERE 
    company IN ('Amazon', 'Netflix')
"""
# Executar a consulta SQL
df_resultado = spark.sql(query)

print("Question 3 - O quão perto a média das notas dos filmes disponíveis na Amazon está dos filmes disponíveis na Netflix?")

# Exibir o resultado do JOIN
df_resultado.show()


# Question 4 - Qual ano de lançamento possui mais filmes na Netflix?
query = """
SELECT 
    count(*) as qtd_filmes,
    year_released
FROM 
    filmes 
WHERE 
    company = 'Netflix'
GROUP BY year_released
ORDER BY qtd_filmes DESC
LIMIT 1
"""
# Executar a consulta SQL
df_resultado = spark.sql(query)

print("Question 4 - Qual ano de lançamento possui mais filmes na Netflix?")


# Exibir o resultado do JOIN
df_resultado.show()

# Question 5 - ○ Quais filmes que não estão disponíveis no catálogo da Amazon foram melhor avaliados?
query = """
SELECT 
    f_netflix.title, 
    a.star_rating as avaliacao
FROM 
    filmes f_netflix
JOIN 
    avaliacoes a ON f_netflix.product_id = a.product_id
WHERE 
    a.star_rating >= 4
AND f_netflix.company = 'Netflix'
AND f_netflix.title NOT IN (
        SELECT f_amazon.title
        FROM filmes f_amazon
        WHERE f_amazon.company = 'Amazon'
    )
GROUP BY 
    f_netflix.title, 
    a.star_rating
ORDER BY 
    star_rating DESC, f_netflix.title ASC;
"""
# Executar a consulta SQL
df_resultado = spark.sql(query)

print("Question 5 - ○ Quais filmes que não estão disponíveis no catálogo da Amazon foram melhor avaliados?")

# Exibir o resultado do JOIN
df_resultado.show(truncate=False)



# Question 6 -Quais filmes que não estão disponíveis no catálogo da Netflix foram melhor avaliados?
query = """
SELECT 
    f_amazon.title, 
    a.star_rating as avaliacao
FROM 
    filmes f_amazon
JOIN 
    avaliacoes a ON f_amazon.product_id = a.product_id
WHERE 
    a.star_rating >= 4
AND f_amazon.company = 'Amazon'
AND f_amazon.title NOT IN (
        SELECT f_netflix.title
        FROM filmes f_netflix
        WHERE f_netflix.company = 'Netflix'
    )
GROUP BY 
    f_amazon.title, 
    a.star_rating
ORDER BY 
    star_rating DESC, f_amazon.title ASC;
"""
# Executar a consulta SQL
df_resultado = spark.sql(query)

print("Question 6 -Quais filmes que não estão disponíveis no catálogo da Netflix foram melhor avaliados?")

# Exibir o resultado do JOIN
df_resultado.show(truncate=False)



# Question 7 -Quais os 10 filmes que possuem mais avaliações nas plataformas?
query = """
SELECT * FROM (
    SELECT 
        COUNT(*) AS qtd_avaliacoes,
        f.title,
        f.company
    FROM 
        filmes f
    JOIN 
        avaliacoes a ON f.product_id = a.product_id
    WHERE 
        f.company = 'Amazon'
    GROUP BY 
        f.title, f.company
    ORDER BY 
        qtd_avaliacoes DESC
    LIMIT 10
) AS amazon_filmes

UNION ALL

SELECT * FROM (
    SELECT 
        COUNT(*) AS qtd_avaliacoes,
        f.title,
        f.company
    FROM 
        filmes f
    JOIN 
        avaliacoes a ON f.product_id = a.product_id
    WHERE 
        f.company = 'Netflix'
    GROUP BY 
        f.title, f.company
    ORDER BY 
        qtd_avaliacoes DESC
    LIMIT 10
) AS netflix_filmes;
"""
# Executar a consulta SQL
df_resultado = spark.sql(query)

print("Question 7 -Quais os 10 filmes que possuem mais avaliações nas plataformas?")


# Exibir o resultado do JOIN
df_resultado.show(truncate=False)




# Question 8 - Quais são os 5 clientes que mais avaliaram filmes na Amazon e quantos produtos diferentes eles avaliaram? E na Netflix?
query = """
select * from (
SELECT 
    a.customer_id,
    COUNT(*) AS qtd_avaliacoes, 
    COUNT(DISTINCT f.product_id) AS qtd_filmes_avaliados,
    f.company
FROM 
    avaliacoes a
JOIN 
    filmes f ON 
        a.product_id = f.product_id
    and a.company = f.company
WHERE 
    f.company = 'Amazon'
GROUP BY 
    a.customer_id, f.company
ORDER BY 
    qtd_avaliacoes DESC
LIMIT 5)amazon
union
select * from (
SELECT 
    a.customer_id,
    COUNT(*) AS qtd_avaliacoes, 
    COUNT(DISTINCT f.product_id) AS qtd_filmes_avaliados,
    f.company
FROM 
    avaliacoes a
JOIN 
    filmes f ON 
        a.product_id = f.product_id
    and a.company = f.company
WHERE 
    f.company = 'Netflix'
GROUP BY 
    a.customer_id, f.company
ORDER BY 
    qtd_avaliacoes DESC
LIMIT 5)netflix
;
"""
# Executar a consulta SQL
df_resultado = spark.sql(query)

print("Question 8 - Quais são os 5 clientes que mais avaliaram filmes na Amazon e quantos produtos diferentes eles avaliaram? E na Netflix?")


# Exibir o resultado do JOIN
df_resultado.show(truncate=False)

# Question 9 - Quantos filmes foram avaliados na data de avaliação mais recente na Amazon?
query = """
SELECT 
    COUNT(DISTINCT f.product_id) AS qtd_filmes_avaliados
FROM 
    avaliacoes a
JOIN 
    filmes f ON a.product_id = f.product_id
WHERE 
    f.company = 'Amazon'
    AND a.review_date = (
        SELECT MAX(a2.review_date)
        FROM avaliacoes a2
        JOIN filmes f2 ON a2.product_id = f2.product_id
        WHERE f2.company = 'Amazon'
    );

"""
# Executar a consulta SQL
df_resultado = spark.sql(query)

print("Question 9 - Quantos filmes foram avaliados na data de avaliação mais recente na Amazon?")


# Exibir o resultado do JOIN
df_resultado.show(truncate=False)

#Question 10 - Quais os 10 filmes mais bem avaliados nesta data?

query = """
SELECT 
    f.title, 
    a.star_rating as avaliacao
FROM 
    avaliacoes a
JOIN 
    filmes f ON a.product_id = f.product_id
WHERE 
    f.company = 'Amazon'
    AND a.review_date = (
        SELECT MAX(a2.review_date)
        FROM avaliacoes a2
        JOIN filmes f2 ON a2.product_id = f2.product_id
        WHERE f2.company = 'Amazon'
    )
ORDER BY 
    star_rating DESC, f.title ASC LIMIT 10;
"""

# Executar a consulta SQL
df_resultado = spark.sql(query)

print("Question 10 - Quais os 10 filmes mais bem avaliados nesta data?")


# Exibir o resultado do JOIN
df_resultado.show(truncate=False)


# Question 11 - Quantos filmes foram avaliados na data de avaliação mais recente na Netflix?
query = """
SELECT 
    COUNT(DISTINCT f.product_id) AS qtd_filmes_avaliados
FROM 
    avaliacoes a
JOIN 
    filmes f ON a.product_id = f.product_id
WHERE 
    f.company = 'Netflix'
    AND a.review_date = (
        SELECT MAX(a2.review_date)
        FROM avaliacoes a2
        JOIN filmes f2 ON a2.product_id = f2.product_id
        WHERE f2.company = 'Netflix'
    );

"""
# Executar a consulta SQL
df_resultado = spark.sql(query)

print("Question 11 - Quantos filmes foram avaliados na data de avaliação mais recente na Netflix?")


# Exibir o resultado do JOIN
df_resultado.show(truncate=False)

#Question 12 - Quais os 10 filmes mais bem avaliados nesta data?

query = """
SELECT 
    f.title, 
    a.star_rating as avaliacao
FROM 
    avaliacoes a
JOIN 
    filmes f ON a.product_id = f.product_id
WHERE 
    f.company = 'Netflix'
    AND a.review_date = (
        SELECT MAX(a2.review_date)
        FROM avaliacoes a2
        JOIN filmes f2 ON a2.product_id = f2.product_id
        WHERE f2.company = 'Netflix'
    )
ORDER BY 
    star_rating DESC, f.title ASC LIMIT 10;
"""

# Executar a consulta SQL
df_resultado = spark.sql(query)

print("Question 12 - Quais os 10 filmes mais bem avaliados nesta data?")


# Exibir o resultado do JOIN
df_resultado.show(truncate=False)



# Encerrar a sessão Spark
spark.stop()