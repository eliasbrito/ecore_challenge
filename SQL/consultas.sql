# Question 1 - Quantos filmes estão disponíveis na Amazon? E na Netflix?
SELECT 
    count(*) qtd_filmes_disponiveis, company 
FROM 
    dw_challenge.product_dim
group by company

# Question 2 - Dos filmes disponíveis na Amazon, quantos % estão disponíveis na Netflix?

SELECT 
        COUNT(distinct CASE WHEN f_netflix.title IS NOT NULL THEN f_amazon.title END) as qtd_netflix,
        COUNT(distinct f_amazon.title) as qtd_amazon,
        round(COUNT(distinct CASE WHEN f_netflix.title IS NOT NULL THEN f_amazon.title END) /COUNT(distinct f_amazon.title) * 100,2) as perc_amazon_netflix        
FROM 
    (SELECT title FROM dw_challenge.product_dim WHERE company = 'Amazon') f_amazon
LEFT JOIN 
    (SELECT title FROM dw_challenge.product_dim WHERE company = 'Netflix') f_netflix
ON f_amazon.title = f_netflix.title;

# Question 3 - O quão perto a média das notas dos filmes disponíveis na Amazon está dos filmes disponíveis na Netflix?
SELECT 
    round(AVG(CASE WHEN pd.company = 'Amazon' THEN rf.star_rating END),2) as media_amazon,
    round(AVG(CASE WHEN pd.company = 'Netflix' THEN rf.star_rating END),2) as media_netflix,
    round(AVG(CASE WHEN pd.company = 'Amazon' THEN rf.star_rating END) / AVG(CASE WHEN pd.company = 'Netflix' THEN rf.star_rating END),2) AS proporcao_amazon_netflix
FROM 
    dw_challenge.rating_fact rf inner join product_dim pd on rf.product_surrogate_key  = pd.surrogate_key 
WHERE 
    pd.company IN ('Amazon', 'Netflix')
    
  
 
# Question 4 - Qual ano de lançamento possui mais filmes na Netflix?

SELECT 
    count(*) as qtd_filmes,
    year_released
FROM 
    product_dim 
WHERE 
    company = 'Netflix'
GROUP BY year_released
ORDER BY qtd_filmes DESC
LIMIT 1



# Question 5 - ○ Quais filmes que não estão disponíveis no catálogo da Amazon foram melhor avaliados?

SELECT 
    f_netflix.title, 
    a.star_rating as avaliacao
FROM 
    product_dim f_netflix
JOIN 
    rating_fact a ON f_netflix.surrogate_key = a.product_surrogate_key 
WHERE 
    a.star_rating >= 4
AND f_netflix.company = 'Netflix'
AND f_netflix.title NOT IN (
        SELECT f_amazon.title
        FROM product_dim f_amazon
        WHERE f_amazon.company = 'Amazon'
    )
GROUP BY 
    f_netflix.title, 
    a.star_rating
ORDER BY 
    star_rating DESC, f_netflix.title ASC;
    
   

# Question 6 -Quais filmes que não estão disponíveis no catálogo da Netflix foram melhor avaliados?
SELECT 
    f_amazon.title, 
    a.star_rating as avaliacao
FROM 
    product_dim f_amazon
JOIN 
    rating_fact a ON f_amazon.surrogate_key = a.product_surrogate_key 
WHERE 
    a.star_rating >= 4
AND f_amazon.company = 'Amazon'
AND f_amazon.title NOT IN (
        SELECT f_netflix.title
        FROM product_dim f_netflix
        WHERE f_netflix.company = 'Netflix'
    )
GROUP BY 
    f_amazon.title, 
    a.star_rating
ORDER BY 
    star_rating DESC, f_amazon.title ASC;
    

# Question 7 -Quais os 10 filmes que possuem mais avaliações nas plataformas?
SELECT * FROM (
    SELECT 
        COUNT(*) AS qtd_avaliacoes,
        f.title,
        f.company
    FROM 
        product_dim f
    JOIN 
        rating_fact a ON f.surrogate_key = a.product_surrogate_key 
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
        product_dim f
    JOIN 
        rating_fact a ON f.surrogate_key = a.product_surrogate_key 
    WHERE 
        f.company = 'Netflix'
    GROUP BY 
        f.title, f.company
    ORDER BY 
        qtd_avaliacoes DESC
    LIMIT 10
) AS netflix_filmes;


# Question 8 - Quais são os 5 clientes que mais avaliaram filmes na Amazon e quantos produtos diferentes eles avaliaram? E na Netflix?
select * from (
SELECT 
    a.customer_id,
    COUNT(*) AS qtd_avaliacoes, 
    COUNT(DISTINCT f.product_id) AS qtd_filmes_avaliados,
    f.company
FROM 
    rating_fact a
JOIN 
    product_dim f ON 
       f.surrogate_key = a.product_surrogate_key 
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
    rating_fact a
JOIN 
    product_dim  f ON 
       f.surrogate_key = a.product_surrogate_key 
WHERE 
    f.company = 'Netflix'
GROUP BY 
    a.customer_id, f.company
ORDER BY 
    qtd_avaliacoes DESC
LIMIT 5)netflix
;

# Question 9 - Quantos filmes foram avaliados na data de avaliação mais recente na Amazon?
SELECT 
    COUNT(DISTINCT f.product_id) AS qtd_filmes_avaliados
FROM 
    rating_fact a
JOIN 
    product_dim  f ON 
       f.surrogate_key = a.product_surrogate_key 
JOIN time_dim t on
    t.surrogate_key = a.time_surrogate_key 
WHERE 
    f.company = 'Amazon'
    AND t.`data` = (
        SELECT MAX(t2.`data`)
        FROM rating_fact a2
        JOIN product_dim f2 ON f2.surrogate_key = a2.product_surrogate_key 
        join time_dim t2 on
            t2.surrogate_key = a2.time_surrogate_key 
        WHERE f2.company = 'Amazon'
    );
    
 
#Question 10 - Quais os 10 filmes mais bem avaliados nesta data?
SELECT 
    f.title, 
    a.star_rating as avaliacao
FROM 
    rating_fact a
JOIN 
    product_dim  f ON 
       f.surrogate_key = a.product_surrogate_key 
JOIN time_dim t on
    t.surrogate_key = a.time_surrogate_key 
WHERE 
    f.company = 'Amazon'
    AND t.`data` = (
        SELECT MAX(t2.`data`)
        FROM rating_fact a2
        JOIN product_dim f2 ON f2.surrogate_key = a2.product_surrogate_key 
        join time_dim t2 on
            t2.surrogate_key = a2.time_surrogate_key 
        WHERE f2.company = 'Amazon'
    );
ORDER BY 
    star_rating DESC, f.title ASC LIMIT 10;
    

# Question 11 - Quantos filmes foram avaliados na data de avaliação mais recente na Netflix?

SELECT 
    COUNT(DISTINCT f.product_id) AS qtd_filmes_avaliados
FROM 
    rating_fact a
JOIN 
    product_dim  f ON 
       f.surrogate_key = a.product_surrogate_key 
JOIN time_dim t on
    t.surrogate_key = a.time_surrogate_key 
WHERE 
    f.company = 'Netflix'
    AND t.`data` = (
              SELECT MAX(t2.`data`)
        FROM rating_fact a2
        JOIN product_dim f2 ON f2.surrogate_key = a2.product_surrogate_key 
        join time_dim t2 on
            t2.surrogate_key = a2.time_surrogate_key 
        WHERE f2.company = 'Netflix'
    );

#Question 12 - Quais os 10 filmes mais bem avaliados nesta data?
SELECT 
    f.title, 
    a.star_rating as avaliacao
FROM 
    rating_fact a
JOIN 
    product_dim  f ON 
       f.surrogate_key = a.product_surrogate_key 
JOIN time_dim t on
    t.surrogate_key = a.time_surrogate_key 
WHERE 
    f.company = 'Netflix'
    AND t.`data` = (
              SELECT MAX(t2.`data`)
        FROM rating_fact a2
        JOIN product_dim f2 ON f2.surrogate_key = a2.product_surrogate_key 
        join time_dim t2 on
            t2.surrogate_key = a2.time_surrogate_key 
        WHERE f2.company = 'Netflix'
    )
    ORDER BY 
    star_rating DESC, f.title ASC LIMIT 10;