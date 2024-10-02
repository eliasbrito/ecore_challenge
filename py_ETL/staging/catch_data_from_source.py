import os
import requests
import json
import zipfile
import sys
import shutil

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config
#
destination_folder = config.LAKE_STG_PATH
os.makedirs(destination_folder, exist_ok=True)

# Caminho para o arquivo kaggle.json (normalmente em ~/.kaggle/kaggle.json)
kaggle_json_path = os.path.expanduser(config.KAGGLE_JSON_PATH)

# Função para remover arquivos no subdiretório "/staging/amazon", exceto os que contêm as partes do nome especificadas
def remove_files_except(directory, allowed_parts):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if not any(part in file for part in allowed_parts):
                file_path = os.path.join(root, file)
                os.remove(file_path)
                print(f"Arquivo removido: {file_path}")

# Ler as credenciais do arquivo kaggle.json
with open(kaggle_json_path, 'r') as file:
    kaggle_credentials = json.load(file)

KAGGLE_USERNAME = kaggle_credentials['username']
KAGGLE_KEY = kaggle_credentials['key']

sources = config.DIC_DATASET

for empresa, caminho in sources.items():
    url = f"https://www.kaggle.com/api/v1/datasets/download/{caminho}"
    print(url)
    response = requests.get(url, stream=True, auth=(KAGGLE_USERNAME, KAGGLE_KEY))
    if response.status_code == 200:
        # Defina o nome do arquivo de destino
        destination_file = os.path.join(destination_folder,f"{empresa}.zip")
        # Escreva o conteúdo em um arquivo
        with open(destination_file, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)

        print(f"Download completo! Arquivo salvo em: {destination_file}")
    
        with zipfile.ZipFile(destination_folder + f'/{empresa}.zip', 'r') as zip_ref:
            zip_ref.extractall(destination_folder + f'{empresa}')
    else:
        print(f"Erro: {response.status_code}, não foi possível baixar o dataset.")
# Função para remover arquivos .zip no diretório "/staging"
def remove_zip_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".zip"):
                file_path = os.path.join(root, file)
                os.remove(file_path)
                print(f"Arquivo removido: {file_path}")

# Definir os diretórios
amazon_directory = destination_folder + "amazon"

# Lista de partes do nome dos arquivos que não devem ser removidos
allowed_parts = ["Video_v1_00", "Video_DVD_v1_00", "Digital_Video_Download_v1_00"]

# Remover arquivos .zip no diretório "/staging"
remove_zip_files(destination_folder)

# Remover arquivos no subdiretório "/staging/amazon", exceto os permitidos
remove_files_except(amazon_directory, allowed_parts)