###############################################################################
# Provider + S3 Data Lake (arquitectura Medallion) + bucket de codigo
#
# NOTA (AWS Academy): la cuenta de laboratorio tiene politicas (SCP) que bloquean
# algunas operaciones de S3 (object lock, versioning, etc.). Por eso aqui creamos
# los buckets de forma MINIMA. En una cuenta AWS normal se podrian añadir
# versionado y cifrado explicito (ver bloques comentados al final).
###############################################################################

terraform {
  required_version = ">= 1.3"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# -----------------------------------------------------------------------------
# Bucket del Data Lake. Dentro viven las zonas por prefijo (carpeta):
#   landing/  bronze/  silver/  gold/  cuarentena/  checkpoints/  athena-results/
# No se crean carpetas vacias: S3 las crea al escribir el primer objeto.
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "datalake" {
  bucket = var.data_bucket

  tags = {
    Proyecto = var.proyecto
    Uso      = "data-lake-medallion"
  }
}

# -----------------------------------------------------------------------------
# Bucket para el codigo de Spark (los .py que EMR ejecuta).
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "codigo" {
  bucket = var.code_bucket
  tags = {
    Proyecto = var.proyecto
    Uso      = "scripts-spark"
  }
}

# -----------------------------------------------------------------------------
# NOTA: En una cuenta AWS estandar (no Academy) se añadiria aqui versionado,
# cifrado explicito y bloqueo de acceso publico. En Academy esas operaciones
# estan bloqueadas por SCP, asi que se omiten. S3 cifra por defecto (SSE-S3).
#
# resource "aws_s3_bucket_versioning" "datalake" { ... }
# resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" { ... }
# resource "aws_s3_bucket_public_access_block" "datalake" { ... }
# -----------------------------------------------------------------------------
