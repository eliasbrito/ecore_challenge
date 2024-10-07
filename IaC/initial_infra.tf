provider "aws" {
  region = "us-east-2" # Altere para a região desejada
}

resource "aws_s3_bucket" "bucket-ecore-challenge" {
  bucket = "bucket-ecore-challenge"

  tags = {
    Name        = "bucket-ecore-challenge"
    Environment = "Dev"
  }
}

# Criar a pasta "data_lake" dentro do bucket
resource "aws_s3_object" "data_lake_folder" {
  bucket = aws_s3_bucket.bucket-ecore-challenge.bucket
  key    = "data_lake/" # O sufixo "/" indica que é uma "pasta"
}

# Definir as subpastas dentro de "data_lake"
locals {
  subfolders = ["staging", "bronze", "silver"]
}

# Criar as subpastas dentro de "data_lake" usando um loop for_each
resource "aws_s3_object" "subfolders" {
    for_each = toset(local.subfolders)
        bucket = aws_s3_bucket.bucket-ecore-challenge.bucket
        key = "data_lake/${each.key}/" # Criar cada subpasta com o sufixo "/"
}

# Criar a pasta "py_ETL" dentro do bucket
resource "aws_s3_object" "py_ETL_folder" {
  bucket = aws_s3_bucket.bucket-ecore-challenge.bucket
  key    = "py_ETL/" # O sufixo "/" indica que é uma "pasta"
}

# Definir as subpastas dentro de "py_ETL"
locals {
  etl_subfolders = ["staging", "bronze", "silver"]
}

# Criar as subpastas dentro de "py_ETL" usando um loop for_each
resource "aws_s3_object" "etl-subfolders" {
    for_each = toset(local.etl_subfolders)
        bucket = aws_s3_bucket.bucket-ecore-challenge.bucket
        key = "py_ETL/${each.key}/" # Criar cada subpasta com o sufixo "/"
} 