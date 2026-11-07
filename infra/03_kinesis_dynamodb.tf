###############################################################################
# DynamoDB (estado operacional)
#
# NOTA IMPORTANTE (AWS Academy): el stream de KINESIS no se gestiona con Terraform.
# El proveedor AWS llama automaticamente a IncreaseStreamRetentionPeriod al crear
# el stream (para fijar 24h), y la SCP de Academy bloquea esa operacion (403), sin
# forma de desactivarla desde el .tf. Por eso el stream se crea a mano con AWS CLI:
#     aws kinesis create-stream --stream-name bikenexus-estaciones --shard-count 2
# y el resto de la infra lo referencia por nombre via var.stream_name.
# En una cuenta AWS estandar, aqui iria el recurso aws_kinesis_stream normal.
###############################################################################

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
