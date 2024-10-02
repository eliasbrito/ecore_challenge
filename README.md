# ecore_challenge
This is a solution to the e-core challenge 


Este projeto tem por objetivo processar dados para que uma empresa de streaming de vídeo possa fazer estudos de
mercado para formular a estratégia de negócio que irão adotar.

Para uma parte do estudo de mercado a empresa precisa fazer análises em cima de dados sobre avaliações de filmes e 
séries que estão disponíveis na Amazon e na Netflix, duas de suas concorrentes diretas.

Para realizar as análises foram disponibilizadas duas bases de dados. Uma para cada concorrente.

Features:
Pode ser executado de forma local ou em nuvem
Possui um script Terraform para criar a estrutura de bucket e pastas no provider AWS
Prevê escrita em data lake em camadas
Os scripts de ETL foram desenvolvidos em PySpark
Arquivo de configuração em Python para centralizar os parâmetros globais do projeto.

Premissas
O ambiente de execução deve contar com:
    Python
    Pip
    PySpark
    Conta/Token/Kaggle.json para download automático das bases de dados
    Terraform
    Spark
    MySQL

Organização
O projeto está organizado dentro de uma pasta raiz, no caso py_ETL, mas pode ser qualquer outra de sua escolha, desde que respeite a estrutura interna de pastas:
/py_ETL
    /staging
    /bronze
    /silver

Os scritps dessas pastas irão processar dados nas respectivas pastas:
/data_lake (este nome pode ser alterado, desde que a estrutura interna seja respeitada)
    /staging
    /bronze
    /silver
O script Terraform está na seguinte pasta:
/IaC

Explicando o código fonte, suas entradas e saídas.
/py_ETL/config.py
    Arquivo de configuração do projeto com todas as variáveis e parâmetros centralizados.
    As variáveis que devem ser configuradas são:
        LAKE_STG_PATH = <Caminho para a pasta onde serão baixados seus arquivos de dados brutos>
        LAKE_BRONZE_PATH = <Caminho para a pasta onde serão persistidos seus arquivos de dados da camada bronze do data-lake>
        LAKE_SILVER_PATH = <Caminho para a pasta onde serão persistidos seus arquivos de dados da camada silver do data-lake>
        MAX_MEMORY = "2g" # Configurações do spark. Ajustar conforme necessidade 
        EXECUTOR_CORES = 4 # Configurações do spark. Ajustar conforme necessidade
        DRIVER_MEMORY = "4g" # Configurações do spark. Ajustar conforme necessidade
        KAGGLE_JSON_PATH = <Caminho de arquivos de credenciais kaggle.json>
        DIC_DATASET = <dicinoário contendo empresa e dataset a ser baixado> 
            Ex.: {"netflix": "netflix-inc/netflix-prize-data","amazon": "cynthiarempel/amazon-us-customer-reviews-dataset"}
            O nome do dataset deve coincidir com o disponibilizado para download pelo Kaggle.
            O nome da empresa será referenciado nos scripts de ETL como pasta para buscar os arquivos baixados
        # Configurações da conexão com o MySQL
            usuario = "<digite aqui seu usuario>"
            database = "<digite aqui o nome do banco>"
            host = "<digite aqui nome ou ip do servidor mysql>"
            port = "<digite aqui a porta do mysql>"
            url = f"jdbc:mysql://{host}:{port}/{database}"
            KEY ="<coloque aqui sua chave>"
            ENCRYPTED_PASSWORD = "<cole aqui sua senha criptografada>"

            JDBC_PATH = "<digite aqui o caminho para seu driver jdbc .jar>"

/py_ETL/criptografa_senha.py
    Script que criptografa sua senha. Ele deve ser executado antes de qualquer outro, pois é com ele que você vai configurar o arquivo /py_ETL/config.py. Este script vai gerar um arquivo config.py na pasta raiz do projeto. Copie seu conteúdo e cole no fim do arquivo /py_ETL/config.py. Após fazer isso você pode apagar o arquivo que foi gerado por este script.

/py_ETL/staging/catch_data_from_source.py
    Arquivo responsável por baixar os datasets da Amazon e Netflix usando a API do Kaggle. 
    As entradas são as variáveis devidamente preenchidas no config.py
    O script baixa os datasets compactados em formato zip para a pasta definida em LAKE_STG_PATH (ex.: /data-lake/staging). Cada zip recebe o nome da empresa definida no dicionário DIC_DATASET.
    Após o download ser concluído ele cria subpastas com o nome das empresas definidos no dicionário DIC_DATASET e descompacta o conteúdo dos arquivos em suas respectivas pastas
    Por fim ele exclui os arquivos .zip e os que não são utilizados no processamento

/py_ETL/bronze/load_movies_netflix.py
    Script que lê especificamente o arquivo /netflix/movie_titles.csv da área de staging definida em  LAKE_STG_PATH
    Este arquivo possui a seguinte estrutura: código do filme, ano de lançamento e título do filme. Porém sem cabeçalho. 
    Ex.: 1,2003,Dinosaur Planet
    O script transforma o dataset em um arquivo parquet com a estrutura: "product_id", "year", "title" e o persiste na pasta "/movies_netflix/" na camada bronze definida em LAKE_BRONZE_PATH


/py_ETL/bronze/load_rating_amazon.py
    Script que lê especificamente os arquivos amazon/amazon_reviews_us_*.tsv da área de staging definida em  LAKE_STG_PATH
    Para mais detalhes da estrutura do arquivo, por favor acesse: https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset
    O script transforma o dataset em um arquivo parquet com toda sua estrutura e o persiste na pasta "/rating_amazon/" na camada bronze definida em LAKE_BRONZE_PATH

/py_ETL/bronze/load_rating_netflix.py
    Script que lê especificamente os arquivos  "netflix/combined_data_*.txt" da área de staging definida em  LAKE_STG_PATH
    Sua estrutura é constituída de uma linha com o código do filme seguido do caracter dois pontos (":") e as linhas que o sucedem são as classificações dadas pelo cliente com a seguinte estrutura: código do cliente, nota e data da avaliação. O arquivo não possui cabeçalhos.
    Ex.: 
        1:
        1488844,3,2005-09-06
    Para mais detalhes da estrutura do arquivo, por favor acesse: https://www.kaggle.com/netflix-inc/netflix-prize-data
    O script transforma o dataset em um arquivo parquet com a seguinte estrutura:
            "product_id", "customer_id", "star_rating", "review_date"
    e o persiste na pasta "/rating_netflix/" na camada bronze definida em LAKE_BRONZE_PATH

/py_ETL/silver/load_all_movies.py
    Script que unifica as estruturas e transformando os arquivos parquet da camada bronze (netflix e amazon) para a seguinte estrutura parquet: product_id, year_released, title e company
    A coluna year_released só é populada com dado válido para os registros da Netflix. Para os demais recebe 0 (zero)
    A coluna company é o metadado que informa a origem do dado, sendo 'Netflix' ou 'Amazon' os valores possíveis. Esta coluna é usada em conjunto com o código do filme (product_id) para formar uma chave única já que existe o risco de termos um mesmo id para empresas diferentes.
    Os parquets resultantes são persistidos em "/silver/movies/" na camada silver definida em LAKE_SILVER_PATH

/py_ETL/silver/load_all_ratings.py
    Script que unifica as estruturas e transformando os arquivos parquet da camada bronze (netflix e amazon) para a seguinte estrutura parquet: "product_id", "customer_id", "star_rating", "review_date", e "company"
    A coluna company é o metadado que informa a origem do dado, sendo 'Netflix' ou 'Amazon' os valores possíveis. Esta coluna é usada em conjunto com o código do filme (product_id) para formar uma chave única já que existe o risco de termos um mesmo id para empresas diferentes.
    Os parquets resultantes são persistidos em "/silver/ratings/" na camada silver definida em LAKE_SILVER_PATH

/py_ETL/silver/load_dim_tempo.py
    Script que cria um arquivo parquet para ser importado na tabela de dimensão tempo do DW

/py_ETL/gold/load_product_dim.py
    Script que popula a tabela de dimensão de produto no DW
/py_ETL/gold/load_time_dim.py
    Script que popula a tabela de dimensão tempo no DW
/py_ETL/gold/load_rating_fact.py
    Script que popula a tabela fato de avaliações de produtos no DW

/py_ETL/answer_questions.py
    Este script contém todas as consultas que respondem aos questionamentos do desafio executando as consultas contra os arquivos parquet da camada silver.


/SQL/dw.sql
    Este arquivo contém um DW pronto com uma pequena amostragem dos dados. Você de restaurá-lo em uma instância MySQL para ter a estrutura na qual o projeto vai escrever os dados se decidir usar a solução com banco de dados.
/SQL/consultas.sql
    Este arquivo contém as consultas SQL contra a base do DW que respondem todos os questionamentos do desafio
    