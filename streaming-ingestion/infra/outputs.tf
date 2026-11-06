###############################################################################
# Outputs: datos que Terraform muestra tras desplegar.
# Son útiles para conectar el job de Spark y para verificar.
###############################################################################

output "kinesis_stream_name" {
  description = "Nombre del stream de Kinesis (lo usa el job de Spark para leer)"
  value       = aws_kinesis_stream.estaciones.name
}

output "kinesis_stream_arn" {
  description = "ARN del stream de Kinesis"
  value       = aws_kinesis_stream.estaciones.arn
}

output "lambda_productor_nombre" {
  description = "Nombre de la Lambda productora"
  value       = aws_lambda_function.productor.function_name
}

output "eventbridge_regla" {
  description = "Nombre de la regla de EventBridge que dispara la Lambda"
  value       = aws_cloudwatch_event_rule.cada_minuto.name
}
