"""
Spark batch: Silver -> Gold (modelo dimensional / star schema) (RF-08)
======================================================================

Qué hace:
  A partir de los viajes LIMPIOS de Silver y el catalogo de estaciones,
  construye un modelo dimensional en estrella (star schema) en la capa Gold:

    Fact_Viajes   : una fila por viaje. Metricas (duracion) + claves a dimensiones.
    Dim_Estacion  : datos de cada estacion (del catalogo).
    Dim_Tiempo    : descompone la fecha/hora en atributos (hora, dia, dia_semana,
                    mes, anio) para poder agrupar y filtrar en los analisis.

Para que sirve (RF-08):
  Permite a los analistas consultar, con Athena, el numero de viajes y la
  duracion promedio por estacion, hora y dia, con respuesta < 5 segundos.
  El star schema evita joins complejos y es rapido para agregaciones.

Cómo se ejecuta (en EMR):
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    batch_gold.py \
    --viajes-silver   s3://bikenexus-datalake/silver/viajes \
    --catalogo-silver s3://bikenexus-datalake/silver/catalogo \
    --gold-path       s3://bikenexus-datalake/gold
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def crear_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bikenexus-batch-gold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def construir_dim_tiempo(viajes):
    """
    Dim_Tiempo: una fila por cada instante de inicio distinto, descompuesto en
    atributos utiles para agrupar (hora, dia, dia de la semana, mes, anio).
    La clave (tiempo_id) es el timestamp truncado a la hora.
    """
    return (
        viajes
        .withColumn("tiempo_id", F.date_trunc("hour", F.col("inicio_ts")))
        .select("tiempo_id")
        .distinct()
        .withColumn("anio", F.year("tiempo_id"))
        .withColumn("mes", F.month("tiempo_id"))
        .withColumn("dia", F.dayofmonth("tiempo_id"))
        .withColumn("hora", F.hour("tiempo_id"))
        # 1=Domingo ... 7=Sabado (estandar de Spark).
        .withColumn("dia_semana", F.dayofweek("tiempo_id"))
        .withColumn("nombre_dia", F.date_format("tiempo_id", "EEEE"))
    )


def main():
    parser = argparse.ArgumentParser(description="Spark batch: Silver -> Gold (star schema)")
    parser.add_argument("--viajes-silver", required=True)
    parser.add_argument("--catalogo-silver", required=True)
    parser.add_argument("--gold-path", required=True)
    args = parser.parse_args()

    spark = crear_spark()
    spark.sparkContext.setLogLevel("WARN")

    viajes = spark.read.format("delta").load(args.viajes_silver)
    catalogo = spark.read.format("delta").load(args.catalogo_silver)

    # --- DIM_ESTACION: datos descriptivos de cada estacion -------------------
    dim_estacion = catalogo.select(
        F.col("estacion_id"),
        "nombre", "lat", "lon", "capacidad", "zona",
    )
    dim_estacion.write.format("delta").mode("overwrite").save(
        f"{args.gold_path}/dim_estacion"
    )

    # --- DIM_TIEMPO: atributos temporales ------------------------------------
    dim_tiempo = construir_dim_tiempo(viajes)
    dim_tiempo.write.format("delta").mode("overwrite").save(
        f"{args.gold_path}/dim_tiempo"
    )

    # --- FACT_VIAJES: una fila por viaje, con claves a las dimensiones --------
    fact_viajes = viajes.select(
        F.col("estacion_origen").alias("estacion_origen_id"),
        F.col("estacion_destino").alias("estacion_destino_id"),
        # Clave a Dim_Tiempo: hora de inicio truncada (igual que en la dimension).
        F.date_trunc("hour", F.col("inicio_ts")).alias("tiempo_id"),
        F.col("inicio_ts"),
        F.col("fin_ts"),
        # Metrica principal: duracion del viaje en minutos.
        (F.col("duracion_seg") / 60.0).alias("duracion_min"),
        # Columnas de particion (anio/mes): aceleran consultas por periodo (RF-08).
        F.year("inicio_ts").alias("anio"),
        F.month("inicio_ts").alias("mes"),
    )
    (
        fact_viajes.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("anio", "mes")
        .save(f"{args.gold_path}/fact_viajes")
    )

    # Resumen para verificar.
    print(f"[gold] Dim_Estacion: {dim_estacion.count()} filas")
    print(f"[gold] Dim_Tiempo:   {dim_tiempo.count()} filas")
    print(f"[gold] Fact_Viajes:  {fact_viajes.count()} filas")


if __name__ == "__main__":
    main()
