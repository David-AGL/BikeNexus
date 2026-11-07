###############################################################################
# Lambdas: productora de streaming (RF-01) y carga batch (RF-02, RF-03)
###############################################################################

# -----------------------------------------------------------------------------
# Empaquetado de cada Lambda en .zip (desde el codigo del repo).
# -----------------------------------------------------------------------------
data "archive_file" "lambda_productor_zip" {
  type        = "zip"
  source_file = "${path.module}/../streaming-ingestion/producer/lambda_producer.py"
  output_path = "${path.module}/build/lambda_producer.zip"
}

data "archive_file" "lambda_carga_batch_zip" {
  type        = "zip"
  source_file = "${path.module}/../batch-ingestion/producer/lambda_carga_batch.py"
  output_path = "${path.module}/build/lambda_carga_batch.zip"
}

# -----------------------------------------------------------------------------
# Lambda productora de streaming (consulta la API y publica en Kinesis).
# -----------------------------------------------------------------------------
resource "aws_lambda_function" "productor" {
  function_name = "${var.proyecto}-productor"
  role          = var.lab_role_arn
  handler       = "lambda_producer.handler"
  # python3.10: el proveedor AWS 4.67 (requerido por Academy) aun no soporta 3.11.
  # El codigo funciona igual en 3.10.
  runtime       = "python3.10"
  timeout       = 30
  memory_size   = 256

  filename         = data.archive_file.lambda_productor_zip.output_path
  source_code_hash = data.archive_file.lambda_productor_zip.output_base64sha256

  environment {
    variables = {
      KINESIS_STREAM_NAME = var.stream_name
      MAX_REINTENTOS      = "5"
    }
  }

  tags = { Proyecto = var.proyecto }
}

# -----------------------------------------------------------------------------
# Lambda de carga batch (descarga/descomprime CSV y catalogo a S3 Landing).
# -----------------------------------------------------------------------------
resource "aws_lambda_function" "carga_batch" {
  function_name = "${var.proyecto}-carga-batch"
  role          = var.lab_role_arn
  handler       = "lambda_carga_batch.handler"
  runtime       = "python3.10" # ver nota en la Lambda productora
  timeout       = 300          # el CSV puede ser grande: mas tiempo para descargar/descomprimir
  memory_size   = 1024

  filename         = data.archive_file.lambda_carga_batch_zip.output_path
  source_code_hash = data.archive_file.lambda_carga_batch_zip.output_base64sha256

  environment {
    variables = {
      LANDING_BUCKET = var.data_bucket
    }
  }

  tags = { Proyecto = var.proyecto }
}
