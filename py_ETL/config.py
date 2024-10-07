# Caminhos de arquivos
#LAKE_STG_PATH = "/Users/elias.brito/Documents/GIT_EliasPessoal/ecore_challenge/data_lake/staging/"
#LAKE_BRONZE_PATH = "/Users/elias.brito/Documents/GIT_EliasPessoal/ecore_challenge/data_lake/bronze/"
#LAKE_SILVER_PATH = "/Users/elias.brito/Documents/GIT_EliasPessoal/ecore_challenge/data_lake/silver/"
#AWS
LAKE_STG_PATH = "s3://bucket-ecore-challenge/data_lake/staging/"
LAKE_BRONZE_PATH = "s3://bucket-ecore-challenge/data_lake/bronze/"
LAKE_SILVER_PATH = "s3://bucket-ecore-challenge/data_lake/silver/"
# Outras variáveis de configuração
MAX_MEMORY = "2g"
EXECUTOR_CORES = 4
DRIVER_MEMORY = "4g"

# Caminho de arquivos de credenciais
KAGGLE_JSON_PATH = "~/.kaggle/kaggle.json"

# Datasets utilizados

DIC_DATASET = {
    "netflix": "netflix-inc/netflix-prize-data",
    "amazon": "cynthiarempel/amazon-us-customer-reviews-dataset"
}


# Configurações da conexão com o MySQL
usuario = "root"
database = "dw_challenge"
host = "localhost"
port = "3306"
url = f"jdbc:mysql://{host}:{port}/{database}"
KEY = "EmMKjb_LKQ4ygM5wB2Mup5sNRwAierUXUOXExuNPchs="
ENCRYPTED_PASSWORD = "gAAAAABm_IAWd3NEpoJl9IMY-w-ilSZ7XVR-B3Ep3tmT8BubcNPzlpYSzITWd4OyWeWOuE9vK2zyLOwyKK3aRLZ34tlxEqXCwA=="

JDBC_PATH = "/Users/elias.brito/hop/2.9/hop/plugins/databases/mysql/lib/mysql-connector-j-8.4.0.jar_mysql8x.jar"