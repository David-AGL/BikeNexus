###############################################################################
# EventBridge: los TRES disparadores del sistema (segun el diagrama)
#   1. Cada 60s   -> Lambda productora (estado de estaciones)   RF-01
#   2. Cada dia   -> Lambda carga batch, modo catalogo          RF-03
#   3. Cada mes   -> Step Functions (flujo de viajes)           ADR-05
###############################################################################

# -----------------------------------------------------------------------------
# 1. Cada 60 segundos -> Lambda productora (RF-01)
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "cada_minuto" {
  name                = "${var.proyecto}-ingesta-cada-minuto"
  description         = "Dispara la Lambda productora de estado de estaciones (RF-01)"
  schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_target" "productor_target" {
  rule      = aws_cloudwatch_event_rule.cada_minuto.name
  target_id = "lambda-productor"
  arn       = aws_lambda_function.productor.arn
}

resource "aws_lambda_permission" "permitir_eb_productor" {
  statement_id  = "AllowEBProductor"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.productor.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.cada_minuto.arn
}

# -----------------------------------------------------------------------------
# 2. Cada dia -> Lambda carga batch en modo "catalogo" (RF-03)
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "cada_dia" {
  name                = "${var.proyecto}-catalogo-diario"
  description         = "Dispara la carga diaria del catalogo de estaciones (RF-03)"
  schedule_expression = "cron(0 1 * * ? *)" # todos los dias a la 1:00 UTC
}

resource "aws_cloudwatch_event_target" "catalogo_target" {
  rule      = aws_cloudwatch_event_rule.cada_dia.name
  target_id = "lambda-carga-batch-catalogo"
  arn       = aws_lambda_function.carga_batch.arn
  # Le pasamos el modo "catalogo" en el input del evento.
  input = jsonencode({ modo = "catalogo" })
}

resource "aws_lambda_permission" "permitir_eb_catalogo" {
  statement_id  = "AllowEBCatalogo"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.carga_batch.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.cada_dia.arn
}

# -----------------------------------------------------------------------------
# 3. Cada mes -> Step Functions (flujo de viajes) (ADR-05)
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "cada_mes" {
  name                = "${var.proyecto}-viajes-mensual"
  description         = "Inicia el flujo mensual de viajes en Step Functions (ADR-05)"
  schedule_expression = "cron(0 2 1 * ? *)" # dia 1 de cada mes a las 2:00 UTC
}

resource "aws_cloudwatch_event_target" "sfn_target" {
  rule     = aws_cloudwatch_event_rule.cada_mes.name
  target_id = "step-functions-batch"
  arn      = aws_sfn_state_machine.batch_mensual.arn
  role_arn = var.lab_role_arn # EventBridge necesita un rol para invocar Step Functions
}
