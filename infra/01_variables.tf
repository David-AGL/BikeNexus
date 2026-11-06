###############################################################################
# Variables globales de la infraestructura de BikeNexus
#
# PREPARACION: estos archivos NO despliegan nada por si solos. El despliegue
# ocurre cuando ejecutas 'terraform apply' en tu cuenta de AWS Academy.
###############################################################################

variable "aws_region" {
  description = "Región de AWS"
  type        = string
  default     = "us-east-1"
}

variable "proyecto" {
  description = "Prefijo para nombrar los recursos"
  type        = string
  default     = "bikenexus"
}

# Nombre del bucket del Data Lake. Debe ser UNICO en todo AWS, por eso se
# recomienda añadir un sufijo (ej. las iniciales del equipo o un numero).
variable "data_bucket" {
  description = "Nombre del bucket S3 del Data Lake (debe ser globalmente unico)"
  type        = string
  default     = "bikenexus-datalake-eafit"
}

variable "code_bucket" {
  description = "Nombre del bucket S3 donde se suben los scripts de Spark"
  type        = string
  default     = "bikenexus-codigo-eafit"
}

# Kinesis
variable "stream_shards" {
  description = "Número de shards del stream de Kinesis (RNF-01)"
  type        = number
  default     = 2
}

variable "stream_retencion_horas" {
  description = "Retención del stream en horas (RNF-05)"
  type        = number
  default     = 24
}

# EMR
variable "emr_release" {
  description = "Versión de EMR"
  type        = string
  default     = "emr-7.1.0"
}

variable "emr_instance_type" {
  description = "Tipo de instancia de EMR (pequeña para AWS Academy)"
  type        = string
  default     = "m5.xlarge"
}

# IAM (AWS Academy: usar el LabRole existente, no crear roles nuevos)
variable "lab_role_arn" {
  description = "ARN del LabRole de AWS Academy (IAM -> Roles -> LabRole)"
  type        = string
}

variable "lab_instance_profile" {
  description = "Nombre del instance profile para EMR en AWS Academy (normalmente 'LabInstanceProfile' o 'EMR_EC2_DefaultRole')"
  type        = string
  default     = "LabInstanceProfile"
}
