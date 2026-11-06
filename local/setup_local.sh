#!/usr/bin/env bash
# =============================================================================
# Crea los recursos de AWS en LocalStack (Kinesis, S3, DynamoDB) para pruebas.
# Ejecutar DESPUÉS de "docker compose up -d".
#
# Uso:  bash setup_local.sh
# =============================================================================
set -e

# Endpoint de LocalStack (el "AWS falso" local).
ENDPOINT="http://localhost:4566"
REGION="us-east-1"

# Credenciales ficticias (LocalStack no las valida, pero el CLI las exige).
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_DEFAULT_REGION="$REGION"

echo "==> Creando stream de Kinesis (2 shards)..."
aws --endpoint-url=$ENDPOINT kinesis create-stream \
  --stream-name bikenexus-estaciones \
  --shard-count 2 2>/dev/null || echo "   (ya existía)"

echo "==> Creando buckets de S3 (Data Lake)..."
aws --endpoint-url=$ENDPOINT s3 mb s3://bikenexus-datalake 2>/dev/null || echo "   (ya existía)"

echo "==> Creando tabla DynamoDB (estado de estaciones + GSI)..."
aws --endpoint-url=$ENDPOINT dynamodb create-table \
  --table-name bikenexus-estado-estaciones \
  --attribute-definitions \
      AttributeName=estacion_id,AttributeType=S \
      AttributeName=clasificacion,AttributeType=S \
      AttributeName=min_estimado_restante,AttributeType=N \
  --key-schema AttributeName=estacion_id,KeyType=HASH \
  --global-secondary-indexes \
      'IndexName=gsi-clasificacion-tiempo,KeySchema=[{AttributeName=clasificacion,KeyType=HASH},{AttributeName=min_estimado_restante,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
  --billing-mode PAY_PER_REQUEST 2>/dev/null || echo "   (ya existía)"

echo ""
echo "==> Listo. Recursos creados en LocalStack:"
echo "    - Kinesis stream : bikenexus-estaciones"
echo "    - S3 bucket      : bikenexus-datalake"
echo "    - DynamoDB table : bikenexus-estado-estaciones"
