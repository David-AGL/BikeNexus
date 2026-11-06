###############################################################################
# Provider + configuración de Terraform
#
# NOTA IMPORTANTE (AWS Academy):
#   La cuenta de laboratorio tiene una SCP que bloquea de forma agresiva la
#   lectura de la "object lock configuration" de S3 (GetBucketObjectLockConfiguration),
#   lo que impide que Terraform CREE, IMPORTE o REFRESQUE buckets S3 (siempre da 403).
#
#   Por eso los buckets del Data Lake NO se gestionan con Terraform: se crean a
#   mano con AWS CLI (ver local/COMANDOS o el README de despliegue) y el resto de
#   la infraestructura los referencia por nombre mediante las variables
#   var.data_bucket y var.code_bucket.
#
#   En una cuenta AWS estandar, aqui irian los recursos aws_s3_bucket normales.
###############################################################################

terraform {
  required_version = ">= 1.3"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.67"
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
