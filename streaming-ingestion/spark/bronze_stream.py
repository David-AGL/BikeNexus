"""
Spark Structured Streaming: Kinesis -> S3 Bronze
================================================

Qué hace (RNF-03, RNF-06):
  1. Lee los eventos en tiempo real desde Kinesis Data Streams.
  2. Los guarda CRUDOS e INMUTABLES en la capa Bronze (Delta Lake sobre S3),
     SIN limpiar ni deduplicar (eso es trabajo de Silver).
  3. Añade METADATOS DE ORIGEN a cada registro (shard, sequence number,
     fecha de ingesta) para la trazabilidad (RNF-06).
  4. Usa CHECKPOINTS en S3: Spark anota hasta qué sequence number leyó de cada
     shard, así tras un fallo reanuda sin perder datos ni duplicar de más
     (RNF-03 / RNF-05).

Cómo se ejecuta (en EMR):
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.1.0,\
com.qubole.spark:spark-sql-kinesis_2.12:1.2.1 \
    bronze_stream.py \
    --stream bikenexus-estaciones \
    --region us-east-1 \
    --bronze-path s3://bikenexus-datalake/bronze/estaciones \
    --checkpoint-path s3://bikenexus-datalake/checkpoints/bronze
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from esquema import ESQUEMA_ESTACION


def crear_spark() -> SparkSession:
    """Crea la sesión de Spark con soporte para Delta Lake."""
    return (
        SparkSession.builder
        .appName("bikenexus-bronze-stream")
        # Extensiones de Delta Lake (transacciones ACID sobre S3).
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def leer_de_kinesis(spark: SparkSession, stream: str, region: str):
    """
    Crea un DataFrame de streaming que lee de Kinesis.

    El conector de Kinesis entrega cada registro con columnas técnicas:
      - data          : el payload (nuestro JSON en bytes)
      - partitionKey  : la estacion_id que usamos al publicar
      - sequenceNumber: posición del registro dentro del shard (trazabilidad)
      - shardId       : en qué shard llegó
      - approximateArrivalTimestamp: cuándo llegó a Kinesis
    """
    return (
        spark.readStream
        .format("kinesis")
        .option("streamName", stream)
        .option("region", region)
        # "TRIM_HORIZON" = empezar desde el registro más antiguo disponible.
        # En producción, tras el primer arranque, el checkpoint manda y esto se ignora.
        .option("startingposition", "TRIM_HORIZON")
        .load()
    )


def transformar_a_bronze(df):
    """
    Convierte los registros crudos de Kinesis al formato de Bronze:
      - parsea el JSON del payload a columnas,
      - conserva TODOS los campos del evento sin filtrar ni deduplicar,
      - añade metadatos de origen (RNF-06).
    """
    # El payload viene en binario; lo pasamos a string y luego a JSON.
    json_df = df.select(
        F.col("data").cast("string").alias("json_str"),
        # Metadatos de origen que da Kinesis (trazabilidad RNF-06):
        F.col("partitionKey").alias("meta_partition_key"),
        F.col("sequenceNumber").alias("meta_sequence_number"),
        F.col("shardId").alias("meta_shard_id"),
        F.col("approximateArrivalTimestamp").alias("meta_kinesis_arrival"),
    )

    # Parseamos el JSON a columnas usando el esquema compartido.
    parsed = json_df.select(
        F.from_json(F.col("json_str"), ESQUEMA_ESTACION).alias("e"),
        "meta_partition_key",
        "meta_sequence_number",
        "meta_shard_id",
        "meta_kinesis_arrival",
    )

    # Aplanamos y añadimos la marca de ingesta a Bronze.
    return parsed.select(
        "e.*",
        "meta_partition_key",
        "meta_sequence_number",
        "meta_shard_id",
        "meta_kinesis_arrival",
        F.lit("bikenexus-estaciones").alias("meta_stream_name"),
        F.current_timestamp().alias("meta_bronze_ingest_ts"),
        # Columna de partición física en S3: facilita consultas y limpieza por día.
        F.to_date(F.current_timestamp()).alias("fecha_ingesta"),
    )


def main():
    parser = argparse.ArgumentParser(description="Spark: Kinesis -> Bronze")
    parser.add_argument("--stream", required=True, help="Nombre del stream de Kinesis")
    parser.add_argument("--region", required=True, help="Región de AWS")
    parser.add_argument("--bronze-path", required=True, help="Ruta S3 de la tabla Bronze")
    parser.add_argument("--checkpoint-path", required=True, help="Ruta S3 del checkpoint")
    parser.add_argument(
        "--trigger-segundos", type=int, default=30,
        help="Intervalo de micro-batch en segundos (default 30)",
    )
    args = parser.parse_args()

    spark = crear_spark()
    spark.sparkContext.setLogLevel("WARN")

    crudo = leer_de_kinesis(spark, args.stream, args.region)
    bronze = transformar_a_bronze(crudo)

    # Escribimos a Bronze en formato Delta, en modo append (inmutable),
    # con checkpoint para la tolerancia a fallos (RNF-03/RNF-05).
    query = (
        bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint_path)
        .partitionBy("fecha_ingesta")
        .trigger(processingTime=f"{args.trigger_segundos} seconds")
        .start(args.bronze_path)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
