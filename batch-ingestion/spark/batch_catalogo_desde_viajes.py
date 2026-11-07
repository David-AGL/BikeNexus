"""
Spark batch: construir el catalogo de estaciones DESDE el CSV de viajes
=======================================================================

Alternativa a batch_catalogo.py para cuando no se dispone de un feed de catalogo
que coincida con la ciudad de los viajes (p. ej. viajes de Jersey City pero el
feed GBFS disponible es de NYC).

El CSV de viajes de Citi Bike ya trae el nombre, el id y las coordenadas de las
estaciones de origen y destino. Este job las EXTRAE y las consolida en un
catalogo (una fila por estacion) en Bronze y Silver. Asi el catalogo siempre
coincide 100% con los viajes y el join posterior no queda vacio.

Transformaciones: union (origen + destino) + deduplicacion por estacion_id.

Cómo se ejecuta (en EMR):
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    batch_catalogo_desde_viajes.py \
    --landing-viajes s3://.../landing/viajes \
    --bronze-path    s3://.../bronze/catalogo \
    --silver-path    s3://.../silver/catalogo
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def crear_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bikenexus-catalogo-desde-viajes")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--landing-viajes", required=True)
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--silver-path", required=True)
    args = parser.parse_args()

    spark = crear_spark()
    spark.sparkContext.setLogLevel("WARN")

    viajes = spark.read.option("header", True).csv(args.landing_viajes)

    # Estaciones de ORIGEN.
    origen = viajes.select(
        F.col("start_station_id").alias("estacion_id"),
        F.col("start_station_name").alias("nombre"),
        F.col("start_lat").cast("double").alias("lat"),
        F.col("start_lng").cast("double").alias("lon"),
    )
    # Estaciones de DESTINO.
    destino = viajes.select(
        F.col("end_station_id").alias("estacion_id"),
        F.col("end_station_name").alias("nombre"),
        F.col("end_lat").cast("double").alias("lat"),
        F.col("end_lng").cast("double").alias("lon"),
    )

    # UNION de ambas + quitar nulos + DEDUP por estacion_id.
    estaciones = (
        origen.unionByName(destino)
        .filter(F.col("estacion_id").isNotNull() & (F.col("estacion_id") != ""))
        .dropDuplicates(["estacion_id"])
        # El CSV no trae capacidad ni zona; asignamos valores razonables:
        #  - capacidad estimada (el CSV no la trae) para poder validar en Silver.
        #  - zona derivada de un prefijo del nombre (barrio aproximado) o 'JC'.
        .withColumn("capacidad", F.lit(30))
        .withColumn("zona", F.coalesce(
            F.regexp_extract(F.col("nombre"), r"^([A-Za-z]+)", 1),
            F.lit("General"),
        ))
    )

    # BRONZE (crudo + metadatos).
    bronze = (estaciones
              .withColumn("meta_archivo_origen", F.input_file_name())
              .withColumn("meta_bronze_ingest_ts", F.current_timestamp()))
    bronze.write.format("delta").mode("overwrite").save(args.bronze_path)

    # SILVER (catalogo vigente).
    silver = bronze.select("estacion_id", "nombre", "lat", "lon", "capacidad", "zona")
    silver.write.format("delta").mode("overwrite").save(args.silver_path)

    print(f"[catalogo-desde-viajes] Estaciones en Silver: {silver.count()}")


if __name__ == "__main__":
    main()
