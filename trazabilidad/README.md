# Trazabilidad y linaje de datos (RNF-06)

RNF-06 exige poder **auditar el linaje de cualquier dato desde Gold hasta su
fuente**. BikeNexus lo logra con TRES mecanismos complementarios.

## 1. `_delta_log` + Time Travel (historial de versiones)

Cada tabla Delta mantiene un registro de transacciones (`_delta_log`) que guarda
el historial de versiones. Permite consultar cómo estaba una tabla en el pasado
(time travel) y conservarlo **al menos 30 días** (RNF-06).

Ver el historial de una tabla:
```sql
-- En Spark SQL
DESCRIBE HISTORY delta.`s3://bikenexus-datalake/silver/estaciones`;
```

Consultar una versión anterior (time travel):
```python
# Por numero de version
df_v3 = spark.read.format("delta").option("versionAsOf", 3).load(ruta)

# Por fecha
df = spark.read.format("delta").option("timestampAsOf", "2026-11-01").load(ruta)
```

Configurar la retención a 30 días (ver `configurar_retencion.py`):
```sql
ALTER TABLE delta.`<ruta>`
SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 30 days',
                   'delta.deletedFileRetentionDuration' = 'interval 30 days');
```

## 2. Metadatos de origen en cada registro

Cada registro lleva columnas que indican de dónde vino. Ya se añaden en los jobs:

### Camino streaming (en `bronze_stream.py`)
| Columna | Qué indica |
|---------|-----------|
| `meta_stream_name` | nombre del stream de Kinesis |
| `meta_shard_id` | shard del que vino el registro |
| `meta_sequence_number` | posición exacta en el shard |
| `meta_kinesis_arrival` | hora de llegada a Kinesis |
| `meta_bronze_ingest_ts` | hora de ingesta a Bronze |

### Camino batch (en `batch_viajes.py` y `batch_catalogo.py`)
| Columna | Qué indica |
|---------|-----------|
| `meta_archivo_origen` | nombre del archivo CSV/JSON de origen |
| `meta_bronze_ingest_ts` | hora de ingesta a Bronze |

Con estos metadatos, para cualquier registro se puede identificar **el mensaje
de Kinesis o el archivo exacto** del que proviene.

## 3. Bronze inmutable

Bronze guarda el dato **crudo original** y nunca se modifica (modo append /
overwrite completo, con versionado de S3 activado). Es la fuente de verdad: si
hay dudas sobre un dato de Silver o Gold, siempre se puede volver a Bronze.

## Cómo auditar el linaje de un dato (ejemplo)

Ver `consultas_linaje.sql`. En resumen:
1. Tomo un registro de Gold (ej. un viaje).
2. Busco en Silver el mismo registro por su clave.
3. Uso `meta_archivo_origen` para saber de qué CSV vino.
4. Confirmo en Bronze que el dato crudo original está ahí.
