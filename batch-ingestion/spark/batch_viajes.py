"""
Spark batch: viajes -> Bronze -> Silver (RF-02, RF-05)
======================================================

Qué hace:
  1. Lee el CSV de viajes desde S3 Landing.
  2. BRONZE: carga cruda SIN PERDIDA de registros (RF-02). El conteo de Bronze
     debe coincidir con el del archivo fuente. Añade metadatos de origen (RNF-06).
  3. SILVER: limpieza con reglas de calidad (RF-05). Aparta a cuarentena:
       - viajes con duracion < 1 minuto o > 24 horas,
       - viajes cuya estacion de origen o destino NO exista en el catalogo.
     En Silver no queda ningun viaje fuera de rango ni con estaciones inexistentes.

Requisito de orden:
  El catalogo (batch_catalogo.py) debe procesarse ANTES, porque aqui validamos
  las estaciones de los viajes contra el catalogo de Silver (RF-05).

Cómo se ejecuta (en EMR):
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    batch_viajes.py \
    --landing-path   s3://bikenexus-datalake/landing/viajes \
    --bronze-path    s3://bikenexus-datalake/bronze/viajes \
    --silver-path    s3://bikenexus-datalake/silver/viajes \
    --cuarentena-path s3://bikenexus-datalake/cuarentena/viajes \
    --catalogo-silver s3://bikenexus-datalake/silver/catalogo
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Limites de duracion validos para un viaje (RF-05).
DURACION_MIN_SEG = 60          # 1 minuto
DURACION_MAX_SEG = 24 * 3600   # 24 horas


def crear_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bikenexus-batch-viajes")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser(description="Spark batch: viajes -> Bronze -> Silver")
    parser.add_argument("--landing-path", required=True)
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--cuarentena-path", required=True)
    parser.add_argument("--catalogo-silver", required=True,
                        help="Ruta del catalogo en Silver (para validar estaciones)")
    args = parser.parse_args()

    spark = crear_spark()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Leer el CSV de viajes. header=True usa la primera fila como nombres.
    crudo = spark.read.option("header", True).csv(args.landing_path)

    conteo_fuente = crudo.count()
    print(f"[viajes] Registros en el archivo fuente: {conteo_fuente}")

    # 2. BRONZE: guardar crudo SIN PERDIDA (RF-02) + metadatos de origen (RNF-06).
    bronze = (
        crudo
        .withColumn("meta_archivo_origen", F.input_file_name())
        .withColumn("meta_bronze_ingest_ts", F.current_timestamp())
    )
    bronze.write.format("delta").mode("overwrite").save(args.bronze_path)

    conteo_bronze = spark.read.format("delta").load(args.bronze_path).count()
    print(f"[viajes] Registros en Bronze: {conteo_bronze}")
    # RF-02: el conteo de Bronze debe coincidir con el de la fuente.
    assert conteo_bronze == conteo_fuente, (
        f"PERDIDA DE DATOS: fuente={conteo_fuente}, bronze={conteo_bronze}"
    )

    # 3. SILVER: normalizar columnas. El dataset Citi Bike trae nombres como
    #    'started_at', 'ended_at', 'start_station_id', 'end_station_id'.
    normalizado = bronze.select(
        F.col("started_at").alias("inicio"),
        F.col("ended_at").alias("fin"),
        F.col("start_station_id").alias("estacion_origen"),
        F.col("end_station_id").alias("estacion_destino"),
        "meta_archivo_origen",
        "meta_bronze_ingest_ts",
    )

    # Calculamos la duracion en segundos a partir de las marcas de tiempo.
    con_duracion = (
        normalizado
        .withColumn("inicio_ts", F.to_timestamp("inicio"))
        .withColumn("fin_ts", F.to_timestamp("fin"))
        .withColumn(
            "duracion_seg",
            F.col("fin_ts").cast("long") - F.col("inicio_ts").cast("long"),
        )
    )

    # 4. Validacion 1 (RF-05): duracion dentro de rango [1 min, 24 h].
    duracion_valida = (
        (F.col("duracion_seg") >= DURACION_MIN_SEG) &
        (F.col("duracion_seg") <= DURACION_MAX_SEG)
    )

    # 5. Validacion 2 (RF-05): estaciones existen en el catalogo.
    catalogo = (
        spark.read.format("delta").load(args.catalogo_silver)
        .select("estacion_id")
    )
    ids_validos = [r["estacion_id"] for r in catalogo.collect()]

    estaciones_validas = (
        F.col("estacion_origen").isin(ids_validos) &
        F.col("estacion_destino").isin(ids_validos)
    )

    # Marcamos cada viaje como valido o no, con el motivo del descarte.
    clasificado = con_duracion.withColumn(
        "es_valido", duracion_valida & estaciones_validas
    ).withColumn(
        "motivo_cuarentena",
        F.when(~duracion_valida, F.lit("duracion_fuera_de_rango"))
         .when(~estaciones_validas, F.lit("estacion_inexistente"))
         .otherwise(F.lit(None)),
    )

    validos = clasificado.filter(F.col("es_valido")).drop(
        "es_valido", "motivo_cuarentena"
    )
    invalidos = clasificado.filter(~F.col("es_valido"))

    # 6. Escribir Silver (validos) y cuarentena (invalidos, apartados — RF-05).
    validos.write.format("delta").mode("overwrite").save(args.silver_path)
    invalidos.write.format("delta").mode("overwrite").save(args.cuarentena_path)

    print(f"[viajes] Validos en Silver: {validos.count()}")
    print(f"[viajes] Apartados en cuarentena: {invalidos.count()}")


if __name__ == "__main__":
    main()
