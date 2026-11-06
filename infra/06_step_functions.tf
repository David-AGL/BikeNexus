###############################################################################
# Step Functions: orquestacion del flujo batch mensual (ADR-05)
#
# Carga el JSON de la maquina de estados y sustituye los placeholders ${...}
# con los recursos reales creados por Terraform.
###############################################################################

resource "aws_sfn_state_machine" "batch_mensual" {
  name     = "${var.proyecto}-batch-mensual"
  role_arn = var.lab_role_arn

  # templatefile reemplaza ${LambdaCargaBatchArn}, ${EmrClusterId}, etc.
  definition = templatefile(
    "${path.module}/../batch-ingestion/orquestacion/state_machine.json",
    {
      LambdaCargaBatchArn = aws_lambda_function.carga_batch.arn
      EmrClusterId        = aws_emr_cluster.spark.id
      CodeBucket          = aws_s3_bucket.codigo.bucket
      DataBucket          = aws_s3_bucket.datalake.bucket
    }
  )

  tags = {
    Proyecto = var.proyecto
  }
}
