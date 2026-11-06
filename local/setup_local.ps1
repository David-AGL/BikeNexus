# =============================================================================
# Crea los recursos de AWS en LocalStack (Kinesis, S3, DynamoDB) — versión WINDOWS.
# Ejecutar en PowerShell DESPUÉS de "docker compose up -d".
#
# Uso:  .\setup_local.ps1
#
# Si PowerShell bloquea la ejecución de scripts, corre primero:
#   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
# =============================================================================

$ErrorActionPreference = "Continue"

# Endpoint de LocalStack (el "AWS falso" local).
$ENDPOINT = "http://localhost:4566"

# Credenciales ficticias (LocalStack no las valida, pero el CLI las exige).
$env:AWS_ACCESS_KEY_ID = "test"
$env:AWS_SECRET_ACCESS_KEY = "test"
$env:AWS_DEFAULT_REGION = "us-east-1"

Write-Host "==> Creando stream de Kinesis (2 shards)..."
aws --endpoint-url=$ENDPOINT kinesis create-stream `
  --stream-name bikenexus-estaciones `
  --shard-count 2

Write-Host "==> Creando bucket de S3 (Data Lake)..."
aws --endpoint-url=$ENDPOINT s3 mb s3://bikenexus-datalake

Write-Host "==> Creando tabla DynamoDB (estado de estaciones + GSI)..."
aws --endpoint-url=$ENDPOINT dynamodb create-table `
  --table-name bikenexus-estado-estaciones `
  --attribute-definitions AttributeName=estacion_id,AttributeType=S AttributeName=clasificacion,AttributeType=S AttributeName=min_estimado_restante,AttributeType=N `
  --key-schema AttributeName=estacion_id,KeyType=HASH `
  --global-secondary-indexes "IndexName=gsi-clasificacion-tiempo,KeySchema=[{AttributeName=clasificacion,KeyType=HASH},{AttributeName=min_estimado_restante,KeyType=RANGE}],Projection={ProjectionType=ALL}" `
  --billing-mode PAY_PER_REQUEST

Write-Host ""
Write-Host "==> Verificando recursos creados..."
aws --endpoint-url=$ENDPOINT kinesis list-streams

Write-Host ""
Write-Host "==> Listo. Recursos en LocalStack:"
Write-Host "    - Kinesis stream : bikenexus-estaciones"
Write-Host "    - S3 bucket      : bikenexus-datalake"
Write-Host "    - DynamoDB table : bikenexus-estado-estaciones"
