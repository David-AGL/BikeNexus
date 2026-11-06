###############################################################################
# Outputs: datos que Terraform muestra al terminar 'terraform apply'.
# Utiles para subir el codigo, lanzar jobs y verificar.
###############################################################################

output "data_bucket" {
  description = "Bucket del Data Lake"
  value       = aws_s3_bucket.datalake.bucket
}

output "code_bucket" {
  description = "Bucket de codigo de Spark"
  value       = aws_s3_bucket.codigo.bucket
}

output "kinesis_stream" {
  description = "Nombre del stream de Kinesis"
  value       = aws_kinesis_stream.estaciones.name
}

output "dynamodb_tabla" {
  description = "Tabla DynamoDB de estado"
  value       = aws_dynamodb_table.estado_estaciones.name
}

output "emr_cluster_id" {
  description = "ID del clúster EMR (para lanzar spark-submit)"
  value       = aws_emr_cluster.spark.id
}

output "lambda_productor" {
  description = "Nombre de la Lambda productora"
  value       = aws_lambda_function.productor.function_name
}

output "lambda_carga_batch" {
  description = "Nombre de la Lambda de carga batch"
  value       = aws_lambda_function.carga_batch.function_name
}

output "step_functions_arn" {
  description = "ARN de la maquina de estados batch"
  value       = aws_sfn_state_machine.batch_mensual.arn
}
