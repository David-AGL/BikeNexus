###############################################################################
# Variables de configuración de la infraestructura de ingesta en streaming
#
# Estas variables permiten ajustar el despliegue sin tocar el resto del código.
# Los valores por defecto están pensados para AWS Academy (recursos pequeños).
###############################################################################

variable "aws_region" {
  description = "Región de AWS donde se despliega todo"
  type        = string
  default     = "us-east-1"
}

variable "proyecto" {
  description = "Prefijo para nombrar los recursos (ayuda a identificarlos)"
  type        = string
  default     = "bikenexus"
}

variable "stream_shards" {
  description = "Número de shards del stream de Kinesis (RNF-01: 2 shards cubren el pico de 330 ev/s)"
  type        = number
  default     = 2
}

variable "stream_retencion_horas" {
  description = "Horas que Kinesis retiene los registros (RNF-05: 24h para poder reprocesar)"
  type        = number
  default     = 24
}

variable "frecuencia_ingesta" {
  description = "Cada cuánto EventBridge dispara la Lambda productora (RF-01: cada 60s)"
  type        = string
  default     = "rate(1 minute)"
}

# En AWS Academy el rol de ejecución ya existe (LabRole). Lo parametrizamos
# para no tener que crear roles IAM (Academy no siempre lo permite).
variable "lambda_role_arn" {
  description = "ARN del rol IAM que usa la Lambda. En AWS Academy suele ser el LabRole."
  type        = string
  default     = ""
}
