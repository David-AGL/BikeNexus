###############################################################################
# Kinesis Data Stream (streaming) + DynamoDB (estado operacional)
###############################################################################

# -----------------------------------------------------------------------------
# KINESIS: recibe los reportes de estado de estaciones (RF-01).
#   2 shards cubren el pico de ~330 ev/s (RNF-01).
#
# NOTA (AWS Academy): no se define 'retention_period' ni 'shard_level_metrics'
# porque la cuenta de laboratorio bloquea (SCP) las operaciones
# IncreaseStreamRetentionPeriod y EnableEnhancedMonitoring. La retencion queda
# en el valor por defecto de Kinesis, que ya es 24h (lo que pide RNF-05).
# -----------------------------------------------------------------------------
resource "aws_kinesis_stream" "estaciones" {
  name        = "${var.proyecto}-estaciones"
  shard_count = var.stream_shards

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    Proyecto = var.proyecto
  }
}

# -----------------------------------------------------------------------------
# DYNAMODB: ultimo estado de cada estacion para el dashboard (RF-07, ADR-04).
#   Clave: estacion_id. GSI: clasificacion + min_estimado_restante.
# -----------------------------------------------------------------------------
resource "aws_dynamodb_table" "estado_estaciones" {
  name         = "${var.proyecto}-estado-estaciones"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "estacion_id"

  attribute {
    name = "estacion_id"
    type = "S"
  }
  attribute {
    name = "clasificacion"
    type = "S"
  }
  attribute {
    name = "min_estimado_restante"
    type = "N"
  }

  global_secondary_index {
    name            = "gsi-clasificacion-tiempo"
    hash_key        = "clasificacion"
    range_key       = "min_estimado_restante"
    projection_type = "ALL"
  }

  tags = {
    Proyecto = var.proyecto
  }
}
