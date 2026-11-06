# Glue Data Catalog + Athena — notas de despliegue (RF-08)

## El punto clave: Delta Lake vs. Athena

Nuestras tablas de Gold se escriben en **Delta Lake**. Athena NO lee tablas Delta
de la misma forma que Parquet plano. Hay **dos caminos** para consultarlas; elige uno:

### Opción A — Crawler de Glue sobre Delta (recomendada, más simple)
AWS Glue tiene un **crawler** que detecta tablas Delta y las registra automáticamente
en el Data Catalog, listas para Athena. No hay que escribir el DDL a mano.

Pasos (consola de AWS):
1. Glue → Crawlers → **Create crawler**.
2. Data source: **S3**, ruta `s3://bikenexus-datalake/gold/`, tipo **Delta Lake**.
3. IAM role: el **LabRole** de AWS Academy.
4. Target database: `bikenexus_gold`.
5. Ejecutar el crawler → crea las tablas `dim_estacion`, `dim_tiempo`, `fact_viajes`.
6. Ya puedes correr las consultas de `02_consultas_analiticas.sql` en Athena.

### Opción B — DDL manual + manifiesto de Delta
Si prefieres registrar las tablas a mano con `01_crear_tablas_gold.sql`:
1. En el job de Spark (`batch_gold.py`), tras escribir cada tabla Delta, genera el
   manifiesto para que Athena lea solo los archivos vigentes:
   ```python
   from delta.tables import DeltaTable
   DeltaTable.forPath(spark, ruta).generate("symlink_format_manifest")
   ```
2. Ejecuta `01_crear_tablas_gold.sql` en Athena (apuntando a los manifiestos).
3. Ejecuta `MSCK REPAIR TABLE` para cargar las particiones.

> Recomendacion para el proyecto: usar la **Opción A (crawler)**. Es menos propensa
> a errores y no requiere mantener el DDL sincronizado con el esquema.

## Configuración de Athena (una vez)

Athena necesita un bucket S3 para guardar los resultados de las consultas:
1. Athena → Settings → **Query result location**:
   `s3://bikenexus-datalake/athena-results/`
2. Seleccionar la base de datos `bikenexus_gold`.
3. Ejecutar las consultas de `02_consultas_analiticas.sql`.

## Verificación de RF-08
- Las consultas deben responder en **< 5 segundos**.
- Si tardan mas, revisa que `fact_viajes` este **particionada por anio/mes** (lo esta)
  y que la consulta filtre por esas particiones cuando aplique.
