# Caminhos de arquivos
LAKE_STG_PATH = "<digite aqui o caminho para sua pasta de staging no data lake>"
LAKE_BRONZE_PATH = "<digite aqui o caminho para sua pasta da camada bronze no data lake>"
LAKE_SILVER_PATH = "<digite aqui o caminho para sua pasta da camada silver no data lake>"
#Exemplos na AWS
#LAKE_STG_PATH = "s3://bucket-ecore-challenge/data_lake/staging/"
#LAKE_BRONZE_PATH = "s3://bucket-ecore-challenge/data_lake/bronze/"
#LAKE_SILVER_PATH = "s3://bucket-ecore-challenge/data_lake/silver/"
# Outras variáveis de configuração
MAX_MEMORY = "2g"
EXECUTOR_CORES = 4
DRIVER_MEMORY = "4g"

# Caminho de arquivos de credenciais
KAGGLE_JSON_PATH = "<coloque aqui o caminho para seu arquivo kaggle.json>"

# Datasets utilizados

DIC_DATASET = {
    "netflix": "netflix-inc/netflix-prize-data",
    "amazon": "cynthiarempel/amazon-us-customer-reviews-dataset"
}


# Configurações da conexão com o MySQL
usuario = "<digite aqui seu usuario>"
database = "<digite aqui o nome do banco>"
host = "<digite aqui nome ou ip do servidor mysql>"
port = "<digite aqui a porta do mysql>"
url = f"jdbc:mysql://{host}:{port}/{database}"
KEY ="<coloque aqui sua chave>"
ENCRYPTED_PASSWORD = "<cole aqui sua senha criptografada>"

JDBC_PATH = "<digite aqui o caminho para seu driver jdbc .jar>"


