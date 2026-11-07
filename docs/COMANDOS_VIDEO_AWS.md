# Comandos para el video — BikeNexus 100% en AWS

Guía lista para grabar. Recorre todo lo que pide el documento del proyecto:
**streaming en tiempo real → Spark en EMR → Lakehouse Bronze/Silver/Gold →
consulta de negocio en Athena → orquestación → dashboard**.

Todos los comandos usan los recursos REALES de la cuenta (verificados):

| Recurso | Valor |
|---|---|
| Cuenta AWS (Academy) | `070802278235` |
| Región | `us-east-1` |
| Bucket data lake | `bikenexus-datalake-athina2026` |
| Bucket código Spark | `bikenexus-codigo-athina2026` |
| Bucket resultados Athena | `bikenexus-athena-results-athina2026` |
| Stream Kinesis | `bikenexus-estaciones` |
| Tabla DynamoDB | `bikenexus-estado-estaciones` |
| Clúster EMR | `j-07259345WZQ6EPRKIP3` |
| Lambda productora | `bikenexus-productor` |
| Step Functions | `arn:aws:states:us-east-1:070802278235:stateMachine:bikenexus-batch-mensual` |
| Base de datos Athena | `bikenexus_gold` |

> **Antes de grabar:** inicia el Lab de AWS Academy y copia las credenciales a
> `~/.aws/credentials`. Si el Lab se reinició, el `cluster-id` de EMR puede
> cambiar; actualízalo con el comando del Paso 0.

---

## Paso 0 — Comprobar que el Lab está activo y los IDs son los correctos

```powershell
# ¿Credenciales activas? Debe mostrar la cuenta 070802278235.
aws sts get-caller-identity

# ID real del clúster EMR (por si cambió tras reiniciar el Lab).
aws emr list-clusters --active --query "Clusters[].[Id,Name,Status.State]" --output table
```
> Narrar: "Estamos autenticados en la cuenta de AWS y el clúster EMR está activo."

---

## Paso 1 — Streaming en tiempo real (RF-01, RNF-03)

### 1a. La Lambda productora publica a Kinesis
```powershell
# Invoca la Lambda una vez (en produccion la dispara EventBridge cada 60s).
aws lambda invoke --function-name bikenexus-productor --cli-binary-format raw-in-base64-out --payload '{}' salida_lambda.json
type salida_lambda.json
```
> Resultado esperado: `{"estaciones_leidas": 2518, "publicados_ok": 2518, "fallidos": 0, ...}`
> Narrar: "2518 estaciones reales de Citi Bike, 0 fallos, publicadas a Kinesis."

### 1b. (opcional) Ver los logs de la Lambda
```powershell
aws logs tail /aws/lambda/bikenexus-productor --since 5m
```

### 1c. El stream de Kinesis está activo
```powershell
aws kinesis describe-stream-summary --stream-name bikenexus-estaciones --query "StreamDescriptionSummary.[StreamName,StreamStatus,OpenShardCount]" --output table
```
> Narrar: "El stream está ACTIVE con sus shards; aquí llegan los eventos."

### 1d. El consumidor procesa Kinesis → clasifica → DynamoDB
Déjalo corriendo en una terminal aparte durante toda la grabación (así el
dashboard tiene datos frescos):
```powershell
cd streaming-ingestion
python consumidor_aws.py --intervalo 60
```
> Narrar: "El consumidor lee de Kinesis, deduplica (RNF-03), aparta inválidos
> (RF-04), clasifica cada estación y escribe el estado en DynamoDB (RF-07)."
> Déjalo 1-2 ciclos y sigue; no lo cierres.

---

## Paso 2 — Spark en EMR: Lakehouse Bronze/Silver/Gold (RF-02, RF-05, RF-08)

> Gold YA está construido y poblado. Para el video basta con **mostrar que los
> steps corrieron COMPLETED**. Solo re-ejecuta la cadena si quieres grabarla en
> vivo (ver "Re-ejecución" al final).

### 2a. Historial de steps del clúster (prueba de que Spark corrió)
```powershell
aws emr list-steps --cluster-id j-07259345WZQ6EPRKIP3 --query "Steps[].[Name,Status.State]" --output table
```
> Narrar: "catálogo → viajes → gold, todos COMPLETED. Son transformaciones no
> triviales: JOIN del star schema, GROUP BY por estación/hora, WINDOW (ranking por zona)."

### 2b. El Lakehouse en S3: las tres capas
```powershell
# Capas del data lake
aws s3 ls s3://bikenexus-datalake-athina2026/

# Gold: el star schema (dimensiones + hechos + agregado)
aws s3 ls s3://bikenexus-datalake-athina2026/gold/
```
> Narrar: "Bronze (crudo sin pérdida), Silver (limpio + cuarentena), Gold
> (modelo dimensional). Formato Delta Lake — tabla abierta."

### 2c. (opcional) Mostrar que Gold tiene datos reales
```powershell
aws s3 ls s3://bikenexus-datalake-athina2026/gold/agg_demanda_estacion/ --recursive
```
> Narrar: "Archivos Parquet reales con el `_delta_log` de Delta."

---

## Paso 3 — Consulta de negocio en Athena (RF-08) ⭐

> Esto es lo que más valora la rúbrica: "capa Gold consultable con una query de
> negocio". Mejor hacerlo en la **consola web de Athena** (más visual), pero
> aquí están los comandos equivalentes por CLI.

### Opción A — Consola web (recomendada para el video)
1. Abre **Athena** en la consola de AWS.
2. Settings → Query result location: `s3://bikenexus-athena-results-athina2026/`
3. Base de datos: `bikenexus_gold`.
4. Pega y ejecuta la pregunta de negocio del E1:

```sql
-- ¿En qué estaciones y horas se concentra la demanda, por zona?
SELECT zona, nombre, hora, num_viajes, duracion_promedio_min, rank_en_zona
FROM bikenexus_gold.agg_demanda_estacion
WHERE rank_en_zona <= 3
ORDER BY zona, rank_en_zona;
```
> Narrar: "Responde en segundos la pregunta de negocio del E1 directamente sobre
> Gold. El `rank_en_zona` viene del window function de Spark (RF-08)."

### Opción B — Por CLI (si prefieres terminal)
```powershell
# 1) Lanza la consulta y guarda el ID de ejecución
$QID = aws athena start-query-execution `
  --query-string "SELECT zona, nombre, hora, num_viajes, duracion_promedio_min, rank_en_zona FROM bikenexus_gold.agg_demanda_estacion WHERE rank_en_zona <= 3 ORDER BY zona, rank_en_zona LIMIT 15" `
  --query-execution-context Database=bikenexus_gold `
  --result-configuration OutputLocation=s3://bikenexus-athena-results-athina2026/ `
  --query "QueryExecutionId" --output text

# 2) Estado (espera a que diga SUCCEEDED)
aws athena get-query-execution --query-execution-id $QID --query "QueryExecution.Status.State" --output text

# 3) Resultados
aws athena get-query-results --query-execution-id $QID --query "ResultSet.Rows[].Data[].VarCharValue" --output text
```

### Otras consultas de negocio que puedes mostrar
```sql
-- Demanda por hora del día (los dos picos de commuter: 8am y 5pm)
SELECT hora, SUM(num_viajes) AS viajes
FROM bikenexus_gold.agg_demanda_estacion
GROUP BY hora ORDER BY hora;

-- Top 10 estaciones más demandadas de toda la red
SELECT nombre, zona, SUM(num_viajes) AS total_viajes,
       ROUND(AVG(duracion_promedio_min), 2) AS dur_prom
FROM bikenexus_gold.agg_demanda_estacion
GROUP BY nombre, zona
ORDER BY total_viajes DESC
LIMIT 10;
```

---

## Paso 4 — Orquestación: Step Functions (equivalente a Airflow)

```powershell
# Estado de la máquina de estados
aws stepfunctions describe-state-machine `
  --state-machine-arn arn:aws:states:us-east-1:070802278235:stateMachine:bikenexus-batch-mensual `
  --query "[name,status]" --output table
```
> Narrar: "El pipeline batch mensual está orquestado con dependencias y
> reintentos (ADR-05). Es el equivalente a Airflow que exige el curso."
> **Para el video:** muéstralo también en la **consola de Step Functions**, donde
> se ve el diagrama del flujo (más visual que el CLI).

---

## Paso 5 — Dashboard (consumo en tiempo real, RF-07, RNF-02)

> Requiere que el consumidor del Paso 1d haya poblado DynamoDB.

### 5a. (comprobación) DynamoDB tiene datos
```powershell
aws dynamodb scan --table-name bikenexus-estado-estaciones --select COUNT
```
> Resultado esperado: `"Count": ~2488`. Narrar: "Estado de ~2500 estaciones en DynamoDB."

### 5b. Levantar el dashboard apuntando a AWS real
```powershell
cd analytics-streaming\dashboard
$env:DYNAMODB_TABLA="bikenexus-estado-estaciones"
$env:AWS_REGION="us-east-1"
$env:AWS_DEFAULT_REGION="us-east-1"
Remove-Item Env:\DYNAMODB_ENDPOINT -ErrorAction SilentlyContinue   # asegura que NO use LocalStack
streamlit run dashboard.py
```
> Se abre en http://localhost:8501. Narrar: "El dashboard lee de DynamoDB en AWS
> real: estaciones vacías/llenas/en riesgo ordenadas por urgencia (RF-07),
> auto-refresco cada 60s (RNF-02)." El dashboard SOLO se *ve* en el navegador;
> los datos viven en la nube.

---

## Resumen del recorrido (lo que el espectador ve, en orden)

1. `get-caller-identity` → estás en AWS.
2. `lambda invoke` → 2518 estaciones a Kinesis (streaming real).
3. `consumidor_aws.py` → Kinesis → DynamoDB (procesamiento).
4. `emr list-steps` → Spark corrió en EMR (COMPLETED).
5. `s3 ls gold/` → Lakehouse Bronze/Silver/Gold en Delta.
6. **Athena** → query de negocio con datos reales (lo más importante).
7. `stepfunctions` → orquestación.
8. **Dashboard** → visualización en tiempo real.

---

## Re-ejecución del batch en vivo (OPCIONAL, solo si quieres grabarlo corriendo)

> ⚠️ Si re-ejecutas, primero limpia las carpetas Delta para evitar el conflicto
> de esquema (`Failed to merge LongType and IntegerType`) que ya diagnosticamos.

```powershell
# 1) Limpiar tablas Delta previas
foreach ($p in @("bronze/catalogo","silver/catalogo","bronze/viajes","silver/viajes","cuarentena/viajes","gold/fact_viajes","gold/dim_estacion","gold/dim_tiempo","gold/agg_demanda_estacion")) {
  aws s3 rm "s3://bikenexus-datalake-athina2026/$p/" --recursive
}

# 2) Catálogo desde los viajes (evita el join vacío JC vs NYC)
aws emr add-steps --cluster-id j-07259345WZQ6EPRKIP3 --steps "Type=Spark,Name=catalogo,Args=[--deploy-mode,cluster,--packages,io.delta:delta-spark_2.12:3.1.0,s3://bikenexus-codigo-athina2026/spark/batch_catalogo_desde_viajes.py,--landing-viajes,s3://bikenexus-datalake-athina2026/landing/viajes,--bronze-path,s3://bikenexus-datalake-athina2026/bronze/catalogo,--silver-path,s3://bikenexus-datalake-athina2026/silver/catalogo]"

# 3) Viajes -> Bronze -> Silver (JOIN con catálogo)
aws emr add-steps --cluster-id j-07259345WZQ6EPRKIP3 --steps "Type=Spark,Name=viajes,Args=[--deploy-mode,cluster,--packages,io.delta:delta-spark_2.12:3.1.0,s3://bikenexus-codigo-athina2026/spark/batch_viajes.py,--landing-path,s3://bikenexus-datalake-athina2026/landing/viajes,--bronze-path,s3://bikenexus-datalake-athina2026/bronze/viajes,--silver-path,s3://bikenexus-datalake-athina2026/silver/viajes,--cuarentena-path,s3://bikenexus-datalake-athina2026/cuarentena/viajes,--catalogo-silver,s3://bikenexus-datalake-athina2026/silver/catalogo]"

# 4) Gold: star schema + agregados (JOIN + GROUP BY + WINDOW)
aws emr add-steps --cluster-id j-07259345WZQ6EPRKIP3 --steps "Type=Spark,Name=gold,Args=[--deploy-mode,cluster,--packages,io.delta:delta-spark_2.12:3.1.0,s3://bikenexus-codigo-athina2026/spark/batch_gold.py,--viajes-silver,s3://bikenexus-datalake-athina2026/silver/viajes,--catalogo-silver,s3://bikenexus-datalake-athina2026/silver/catalogo,--gold-path,s3://bikenexus-datalake-athina2026/gold]"

# 5) Regenerar el manifiesto para que Athena lea Gold (Delta)
aws emr add-steps --cluster-id j-07259345WZQ6EPRKIP3 --steps "Type=Spark,Name=manifiesto,Args=[--deploy-mode,cluster,--packages,io.delta:delta-spark_2.12:3.1.0,s3://bikenexus-codigo-athina2026/spark/generar_manifiesto_emr.py,--gold-path,s3://bikenexus-datalake-athina2026/gold]"

# Ver el estado de cualquier step (reemplaza <STEP_ID>)
aws emr describe-step --cluster-id j-07259345WZQ6EPRKIP3 --step-id <STEP_ID> --query "Step.Status.State" --output text
```

> Si después de re-ejecutar la query de Athena saliera vacía, es porque el
> manifiesto se regeneró: vuelve a correr el Paso 5 (manifiesto) y re-ejecuta la
> query. Con los datos ya cargados (como están ahora), no necesitas nada de esto.
