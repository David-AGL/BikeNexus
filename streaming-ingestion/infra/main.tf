###############################################################################
# Infraestructura de ingesta en streaming — BikeNexus
#
# Crea: Kinesis Data Stream + Lambda productora + regla de EventBridge.
# Cubre los requisitos RF-01 (ingesta cada 60s hacia Kinesis) y parte de RNF-03.
###############################################################################

terraform {
  required_version = ">= 1.3"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Datos de la cuenta/región actual (para construir ARNs sin escribirlos a mano).
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

###############################################################################
# 1. KINESIS DATA STREAM  (Paso 2 del diagrama)
#    - 2 shards: cubren el pico de ~330 eventos/s (RNF-01)
#    - retención 24h: permite reprocesar tras un fallo de Spark (RNF-05)
###############################################################################
resource "aws_kinesis_stream" "estaciones" {
  name             = "${var.proyecto}-estaciones"
  shard_count      = var.stream_shards
  retention_period = var.stream_retencion_horas

  # Métricas útiles para las pruebas de carga (RNF-01).
  shard_level_metrics = [
    "IncomingRecords",
    "OutgoingRecords",
    "WriteProvisionedThroughputExceeded",
  ]

  stream_mode_details {
    stream_mode = "PROVISIONED" # shards fijos (más barato y predecible que ON_DEMAND en Academy)
  }

  tags = {
    Proyecto  = var.proyecto
    Componente = "ingesta-streaming"
  }
}

###############################################################################
# 2. LAMBDA PRODUCTORA  (Paso 1 del diagrama)
#    Empaqueta el código de producer/ y lo despliega.
###############################################################################

# Empaqueta lambda_producer.py en un .zip que Lambda pueda ejecutar.
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../producer/lambda_producer.py"
  output_path = "${path.module}/build/lambda_producer.zip"
}

resource "aws_lambda_function" "productor" {
  function_name = "${var.proyecto}-productor"
  role          = var.lambda_role_arn
  handler       = "lambda_producer.handler"
  runtime       = "python3.11"
  timeout       = 30 # suficiente para leer la API y publicar ~2000 estaciones
  memory_size   = 256

  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  environment {
    variables = {
      KINESIS_STREAM_NAME = aws_kinesis_stream.estaciones.name
      MAX_REINTENTOS      = "5"
    }
  }

  tags = {
    Proyecto   = var.proyecto
    Componente = "ingesta-streaming"
  }
}

###############################################################################
# 3. EVENTBRIDGE  (Paso 2 del diagrama)
#    Dispara la Lambda productora cada 60 segundos (RF-01).
###############################################################################
resource "aws_cloudwatch_event_rule" "cada_minuto" {
  name                = "${var.proyecto}-ingesta-cada-minuto"
  description         = "Dispara la Lambda productora de estado de estaciones (RF-01)"
  schedule_expression = var.frecuencia_ingesta
}

# Conecta la regla con la Lambda (el "target" de la regla).
resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.cada_minuto.name
  target_id = "lambda-productor"
  arn       = aws_lambda_function.productor.arn
}

# Permiso para que EventBridge pueda invocar la Lambda.
resource "aws_lambda_permission" "permitir_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.productor.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.cada_minuto.arn
}
