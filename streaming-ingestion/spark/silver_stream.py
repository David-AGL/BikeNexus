"""
Spark Structured Streaming: Bronze -> Silver (limpieza + deduplicación)
======================================================================

Qué hace (RF-04, RNF-03):
  1. Lee en micro-batches (cada 30 s) los registros nuevos de la capa Bronze.
  2. DEDUPLICA por (estacion_id, timestamp): en Silver no puede existir más de
     un registro con esa combinación. Esto convierte el "at-least-once" del
     productor en un efecto "exactly-once" en Silver (RNF-03).
  3. APARTA los registros INVÁLIDOS a una zona de cuarentena (RF-04):
       - bikes_disponibles > capacidad   (físicamente imposible)
       - capacidad == 0                  (estación mal configurada)
     Los inválidos NO se borran: se guardan aparte para su revisión.
  4. Escribe en Silver con MERGE (upsert) de Delta Lake, que es idempotente:
     aunque un micro-batch se reprocese tras un fallo, nunca deja duplicados.

Por qué leer de Bronze y no de Kinesis:
  Bronze es la fuente inmutable y ya trae los metadatos de origen. Separar
  Bronze (crudo) de Silver (limpio) permite reprocesar Silver sin volver a
  leer de Kinesis (arquitectura Medallion).

Cómo se ejecuta (en EMR):
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    silver_stream.py \
    --bronze-path s3://bikenexus-datalake/bronze/estaciones \
    --silver-path s3://bikenexus-datalake/silver/estaciones \
    --cuarentena-path s3://bikenexus-datalake/cuarentena/estaciones \
    --checkpoint-path s3://bikenexus-datalake/checkpoints/silver
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from delta.tables import DeltaTable


def crear_spark() -> SparkSession:
    """Crea la sesión de Spark con soporte para Delta Lake."""
    return (
        SparkSession.builder
        .appName("bikenexus-silver-stream")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


# --- Reglas de validez (RF-04) -----------------------------------------------
def marcar_validez(df: DataFrame) -> DataFrame:
    """
    Añade una columna booleana 'es_valido' aplicando las reglas de RF-04.
    Un registro es INVÁLIDO si:
      - capacidad es nula o 0, o
      - bikes_disponibles > capacidad.
    """
    condicion_invalido = (
        F.col("capacidad").isNull()
        | (F.col("capacidad") <= 0)
        | (F.col("bikes_disponibles") > F.col("capacidad"))
    )
    return df.withColumn("es_valido", ~condicion_invalido)


# --- Deduplicación + escritura (RF-04, RNF-03) -------------------------------
def procesar_micro_batch(batch_df: DataFrame, batch_id: int,
                         silver_path: str, cuarentena_path: str):
    """
    Se ejecuta en CADA micro-batch (foreachBatch). Recibe los registros nuevos
    de Bronze y los reparte entre Silver (válidos) y cuarentena (inválidos).

    Usamos foreachBatch porque permite hacer un MERGE de Delta (upsert
    idempotente), que no está disponible directamente en writeStream.
    """
    # 1. Clasificar válido / inválido.
    clasificado = marcar_validez(batch_df)

    # 2. Dedup DENTRO del batch por (estacion_id, timestamp): si el mismo
    #    reporte vino repetido en este lote, nos quedamos con uno solo.
    ventana = ["estacion_id", "timestamp"]
    validos = (
        clasificado.filter(F.col("es_valido"))
        .dropDuplicates(ventana)
    )
    invalidos = clasificado.filter(~F.col("es_valido"))

    spark = batch_df.sparkSession

    # 3. Escribir los VÁLIDOS en Silver con MERGE (upsert idempotente).
    #    Clave de merge: (estacion_id, timestamp) = clave de dedup del RNF-03.
    if DeltaTable.isDeltaTable(spark, silver_path):
        tabla_silver = DeltaTable.forPath(spark, silver_path)
        (
            tabla_silver.alias("t")
            .merge(
                validos.alias("s"),
                "t.estacion_id = s.estacion_id AND t.timestamp = s.timestamp",
            )
            # Si ya existe ese (estacion_id, timestamp), no duplicamos
            # (opcionalmente se podría actualizar; aquí lo dejamos inmutable).
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # Primera vez: la tabla Silver aún no existe, la creamos.
        validos.write.format("delta").mode("append").save(silver_path)

    # 4. Guardar los INVÁLIDOS en cuarentena (apartados, no borrados — RF-04).
    if not invalidos.rdd.isEmpty():
        (
            invalidos
            .withColumn("motivo_cuarentena",
                        F.when(F.col("capacidad").isNull() | (F.col("capacidad") <= 0),
                               F.lit("capacidad_invalida"))
                         .otherwise(F.lit("bikes_mayor_que_capacidad")))
            .write.format("delta").mode("append").save(cuarentena_path)
        )


def main():
    parser = argparse.ArgumentParser(description="Spark: Bronze -> Silver")
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--cuarentena-path", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--trigger-segundos", type=int, default=30)
    args = parser.parse_args()

    spark = crear_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Leemos Bronze como stream (Delta soporta lectura en streaming).
    bronze_stream = (
        spark.readStream
        .format("delta")
        .load(args.bronze_path)
    )

    # foreachBatch nos deja aplicar MERGE por cada micro-batch.
    query = (
        bronze_stream.writeStream
        .foreachBatch(
            lambda df, bid: procesar_micro_batch(
                df, bid, args.silver_path, args.cuarentena_path
            )
        )
        .option("checkpointLocation", args.checkpoint_path)
        .trigger(processingTime=f"{args.trigger_segundos} seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
