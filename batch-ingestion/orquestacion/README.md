# Orquestación del camino batch — Step Functions (ADR-05)

`state_machine.json` define la máquina de estados que coordina el flujo mensual.

## Flujo

```
EventBridge (mensual) -> Step Functions:
  1. DescargarYDescomprimirCSV  (Lambda carga batch, modo "viajes")   RF-02
  2. ProcesarCatalogo           (Spark en EMR: catalogo -> Silver)    RF-03
  3. ProcesarViajes             (Spark en EMR: viajes  -> Silver)     RF-02, RF-05
  4. ConstruirGold              (Spark en EMR: star schema)           RF-08
```

El orden importa: el catálogo se procesa **antes** que los viajes, porque los
viajes se validan contra él (RF-05). Cada paso tiene **reintentos** con backoff.

## Placeholders a reemplazar

El JSON usa placeholders `${...}` que se rellenan al desplegar (Terraform los
sustituye automáticamente, o se reemplazan a mano):

| Placeholder | Qué es |
|-------------|--------|
| `${LambdaCargaBatchArn}` | ARN de la Lambda de carga batch |
| `${EmrClusterId}` | ID del clúster EMR (ej. `j-XXXXXXXX`) |
| `${CodeBucket}` | Bucket S3 donde están los scripts de Spark |
| `${DataBucket}` | Bucket S3 del Data Lake (landing/bronze/silver/gold) |

## Cómo se despliega

1. Subir los scripts de Spark a S3:
   ```
   aws s3 cp batch-ingestion/spark/ s3://${CodeBucket}/spark/ --recursive
   ```
2. Crear el clúster EMR (ver infra — Paso 15).
3. Crear la máquina de estados con el JSON (lo hace Terraform en el Paso 15, o
   manualmente en la consola de Step Functions).
4. EventBridge dispara la máquina una vez al mes (`cron(0 2 1 * ? *)` = día 1 a las 2am).

## Por qué Step Functions y no Airflow

Para un flujo de pocos pasos con dependencias simples, Step Functions es
administrado, sin servidores que operar, y se integra nativamente con Lambda y
EMR. Airflow (MWAA) seria desproporcionado en costo y administracion (ADR-05).
