###############################################################################
# Tabla DynamoDB para el estado operacional de las estaciones (RF-07, ADR-04)
#
# Guarda SOLO el ultimo estado conocido de cada estacion (no el historico).
# El dashboard la consulta para mostrar las alertas sin tocar el Data Lake.
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

provider "aws" {
  region = var.aws_region
}

resource "aws_dynamodb_table" "estado_estaciones" {
  name         = "${var.proyecto}-estado-estaciones"
  billing_mode = "PAY_PER_REQUEST" # sin aprovisionar capacidad: ideal para Academy

  # Clave primaria: una fila por estacion (upsert sobre estacion_id).
  hash_key = "estacion_id"

  attribute {
    name = "estacion_id"
    type = "S"
  }

  # Atributos que participan en el indice secundario (GSI).
  attribute {
    name = "clasificacion"
    type = "S"
  }

  attribute {
    name = "min_estimado_restante"
    type = "N"
  }

  # Indice secundario global (GSI) para RF-07:
  #   partition key = clasificacion (VACIA / LLENA / EN_RIESGO / NORMAL)
  #   sort key      = min_estimado_restante (tiempo que le queda)
  # Permite al dashboard pedir "todas las EN_RIESGO ordenadas por urgencia"
  # sin escanear la tabla completa.
  global_secondary_index {
    name            = "gsi-clasificacion-tiempo"
    hash_key        = "clasificacion"
    range_key       = "min_estimado_restante"
    projection_type = "ALL"
  }

  tags = {
    Proyecto   = var.proyecto
    Componente = "analitica-streaming"
  }
}

output "dynamodb_tabla" {
  description = "Nombre de la tabla DynamoDB de estado"
  value       = aws_dynamodb_table.estado_estaciones.name
}

output "dynamodb_gsi" {
  description = "Nombre del indice secundario para el dashboard"
  value       = "gsi-clasificacion-tiempo"
}
