###############################################################################
# Provider + S3 Data Lake (arquitectura Medallion) + bucket de codigo
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

# Versionado: ayuda a la trazabilidad e inmutabilidad de Bronze (RNF-06).
resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Cifrado en reposo (seguridad).
resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Bloquear acceso publico (seguridad).
resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
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

resource "aws_s3_bucket_public_access_block" "codigo" {
  bucket                  = aws_s3_bucket.codigo.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
