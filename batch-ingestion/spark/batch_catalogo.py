"""
Spark batch: catálogo de estaciones -> Bronze -> Silver (RF-03)
===============================================================

Qué hace:
  1. Lee el catalogo de estaciones (JSON) desde S3 Landing.
  2. Lo carga CRUDO en Bronze (con metadatos de origen).
  3. Lo consolida en Silver como el catalogo VIGENTE (una fila por estacion).

Por que se procesa ANTES que los viajes:
  Los viajes se validan contra este catalogo (RF-05): un viaje cuya estacion de
  origen o destino no exista aqui se aparta. Por eso el catalogo debe estar
  en Silver antes de procesar los viajes.

Cómo se ejecuta (en EMR):
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    batch_catalogo.py \
    --landing-path s3://bikenexus-datalake/landing/catalogo \
    --bronze-path  s3://bikenexus-datalake/bronze/catalogo \
    --silver-path  s3://bikenexus-datalake/silver/catalogo
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def crear_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bikenexus-batch-catalogo")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser(description="Spark batch: catalogo -> Bronze -> Silver")
    parser.add_argument("--landing-path", required=True, help="Ruta S3 del JSON en Landing")
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--silver-path", required=True)
    args = parser.parse_args()

    spark = crear_spark()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Leer el JSON del catalogo. El feed GBFS anida las estaciones en
    #    data.stations, asi que lo explotamos a una fila por estacion.
    crudo = spark.read.option("multiLine", True).json(args.landing_path)
    estaciones = (
        crudo.select(F.explode("data.stations").alias("e"))
        .select(
            F.col("e.station_id").alias("estacion_id"),
            F.col("e.name").alias("nombre"),
            F.col("e.lat").alias("lat"),
            F.col("e.lon").alias("lon"),
            F.col("e.capacity").alias("capacidad"),
            # 'region_id' o 'zona' segun el feed; lo dejamos opcional.
            F.col("e.region_id").alias("zona"),
        )
    )

    # 2. BRONZE: guardamos crudo con metadatos de origen (RNF-06).
    bronze = (
        estaciones
        .withColumn("meta_archivo_origen", F.input_file_name())
        .withColumn("meta_bronze_ingest_ts", F.current_timestamp())
    )
    bronze.write.format("delta").mode("overwrite").save(args.bronze_path)

    # 3. SILVER: catalogo vigente, una fila por estacion (la mas reciente).
    #    Deduplicamos por estacion_id por si el feed trae repetidos.
    silver = bronze.dropDuplicates(["estacion_id"]).select(
        "estacion_id", "nombre", "lat", "lon", "capacidad", "zona"
    )
    silver.write.format("delta").mode("overwrite").save(args.silver_path)

    total = silver.count()
    print(f"[catalogo] Estaciones en Silver: {total}")


if __name__ == "__main__":
    main()
