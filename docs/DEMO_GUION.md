# Guion de la demo en vivo (8 min) — BikeNexus

Mapea cada punto que pide la rúbrica del E2 con qué mostrar y los comandos.

> Antes de grabar: inicia el Lab de Academy, copia credenciales a `~/.aws/credentials`,
> y despliega con `cd infra && terraform apply`. Crea los buckets S3 a mano
> (`aws s3 mb s3://<data_bucket>` y `<code_bucket>`) y sube el codigo de Spark
> (`aws s3 cp ... s3://<code_bucket>/spark/ --recursive`).

## 1. Streaming en tiempo real (rúbrica: "productor generando eventos")
Muestra la Lambda ingiriendo cada 60s:
```powershell
aws logs tail /aws/lambda/bikenexus-productor --since 5m
```
> Narrar: 2518 estaciones reales de Citi Bike, 0 fallos, cada 60s → Kinesis (RF-01, RNF-03).

Procesamiento en vivo Kinesis → DynamoDB:
```powershell
cd streaming-ingestion
python consumidor_aws.py --intervalo 60
```
> Narrar: dedup (RNF-03), aparta inválidos (RF-04), escribe estado en DynamoDB (RF-07).

## 2. Job de Spark corriendo (rúbrica: "job de Spark mostrando procesamiento")
Lanza el Spark batch en EMR (catálogo → viajes → Gold). Primero descarga un CSV
de viajes a S3 Landing, luego:
```powershell
# Catalogo -> Silver
aws emr add-steps --cluster-id <EMR_ID> --steps "Type=Spark,Name=catalogo,Args=[--deploy-mode,cluster,--packages,io.delta:delta-spark_2.12:3.1.0,s3://<CODE>/spark/batch_catalogo.py,--landing-path,s3://<DATA>/landing/catalogo,--bronze-path,s3://<DATA>/bronze/catalogo,--silver-path,s3://<DATA>/silver/catalogo]"

# Viajes -> Silver (JOIN con catalogo)
aws emr add-steps --cluster-id <EMR_ID> --steps "Type=Spark,Name=viajes,Args=[--deploy-mode,cluster,--packages,io.delta:delta-spark_2.12:3.1.0,s3://<CODE>/spark/batch_viajes.py,--landing-path,s3://<DATA>/landing/viajes,--bronze-path,s3://<DATA>/bronze/viajes,--silver-path,s3://<DATA>/silver/viajes,--cuarentena-path,s3://<DATA>/cuarentena/viajes,--catalogo-silver,s3://<DATA>/silver/catalogo]"

# Gold: star schema + agregados (JOIN + GROUP BY + WINDOW)
aws emr add-steps --cluster-id <EMR_ID> --steps "Type=Spark,Name=gold,Args=[--deploy-mode,cluster,--packages,io.delta:delta-spark_2.12:3.1.0,s3://<CODE>/spark/batch_gold.py,--viajes-silver,s3://<DATA>/silver/viajes,--catalogo-silver,s3://<DATA>/silver/catalogo,--gold-path,s3://<DATA>/gold]"
```
Muestra el estado del step (RUNNING → COMPLETED):
```powershell
aws emr list-steps --cluster-id <EMR_ID> --query "Steps[].[Name,Status.State]" --output table
```
> Narrar las transformaciones no triviales: JOIN del star schema, GROUP BY de
> viajes por estación/hora, WINDOW (ranking por zona).

## 3. Capa Gold consultable (rúbrica: "query analítica de negocio")
Registra Gold con un crawler de Glue (ver athena/03_crawler_glue.md) y en Athena:
```sql
-- Pregunta de negocio E1: ¿en qué zonas se concentra la demanda?
SELECT zona, nombre, hora, num_viajes, duracion_promedio_min, rank_en_zona
FROM bikenexus_gold.agg_demanda_estacion
WHERE rank_en_zona <= 3
ORDER BY zona, rank_en_zona;
```
> Narrar: responde directamente una pregunta de negocio del E1 en segundos (RF-08).

## 4. Orquestación (rúbrica: "DAG mostrando el pipeline")
Muestra Step Functions en la consola de AWS (o por CLI):
```powershell
aws stepfunctions describe-state-machine --state-machine-arn <SFN_ARN> --query "[name,status]" --output table
```
> Narrar: orquesta el flujo mensual con dependencias y reintentos (ADR-05, equivalente a Airflow).

## 5. Dashboard (consumo en tiempo real)
```powershell
cd analytics-streaming\dashboard
$env:DYNAMODB_TABLA="bikenexus-estado-estaciones"; $env:AWS_DEFAULT_REGION="us-east-1"
streamlit run dashboard.py
```
> Narrar: alertas ordenadas por urgencia, auto-refresco cada 60s (RF-07, RNF-02).

## Si algo falla en vivo (plan de defensa)
- **Spark-Kinesis**: explicar que el conector es de terceros y falla en Academy;
  por eso el streaming se consume con boto3 (misma lógica) y Spark se demuestra
  en el camino batch (viajes→Gold), que SÍ usa transformaciones no triviales.
- **EMR lento/caído**: mostrar los jobs corriendo en local con Spark, o los
  resultados ya escritos en S3/Gold.
