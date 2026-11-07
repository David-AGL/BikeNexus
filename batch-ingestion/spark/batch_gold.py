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
    # overwriteSchema=true en todas las escrituras Gold: hace el job
    # re-ejecutable aunque el esquema de una tabla Delta previa difiera.
    dim_estacion.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(
        f"{args.gold_path}/dim_estacion"
    )

    # --- DIM_TIEMPO: atributos temporales ------------------------------------
    dim_tiempo = construir_dim_tiempo(viajes)
    dim_tiempo.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(
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
        .option("overwriteSchema", "true")
        .partitionBy("anio", "mes")
        .save(f"{args.gold_path}/fact_viajes")
    )

    # --- GOLD AGREGADO: transformaciones NO TRIVIALES (rubrica: join + groupBy
    #     + windowing). Responde preguntas de negocio del E1 directamente. -----
    construir_agregados_negocio(spark, fact_viajes, dim_estacion, dim_tiempo,
                                args.gold_path)

    # Resumen para verificar.
    print(f"[gold] Dim_Estacion: {dim_estacion.count()} filas")
    print(f"[gold] Dim_Tiempo:   {dim_tiempo.count()} filas")
    print(f"[gold] Fact_Viajes:  {fact_viajes.count()} filas")


def construir_agregados_negocio(spark, fact, dim_estacion, dim_tiempo, gold_path):
    """
    Tabla Gold de agregados que responde preguntas de negocio del E1, usando las
    TRES transformaciones NO TRIVIALES que exige la rubrica:

      1) JOIN   : fact_viajes  x  dim_estacion  x  dim_tiempo (star join).
      2) GROUP BY: agrega numero de viajes y duracion promedio por estacion, zona,
                   dia de la semana y hora.
      3) WINDOWING: ranking de las estaciones mas demandadas por zona (window
                    function row_number sobre una particion por zona).

    Resultado: tabla 'gold/agg_demanda_estacion' lista para Athena.
    """
    from pyspark.sql.window import Window

    # (1) JOIN del star schema: hechos con las dos dimensiones.
    enriquecido = (
        fact.alias("f")
        .join(dim_estacion.alias("e"), F.col("f.estacion_origen_id") == F.col("e.estacion_id"), "inner")
        .join(dim_tiempo.alias("t"), F.col("f.tiempo_id") == F.col("t.tiempo_id"), "inner")
    )

    # (2) GROUP BY: metricas de negocio por estacion / zona / dia / hora.
    agregado = (
        enriquecido.groupBy(
            F.col("e.estacion_id"), F.col("e.nombre"), F.col("e.zona"),
            F.col("t.nombre_dia"), F.col("t.hora"),
        )
        .agg(
            F.count("*").alias("num_viajes"),
            F.round(F.avg("f.duracion_min"), 2).alias("duracion_promedio_min"),
            F.round(F.max("f.duracion_min"), 2).alias("duracion_max_min"),
        )
    )

    # (3) WINDOWING: ranking de estaciones mas demandadas dentro de cada zona.
    ventana_zona = Window.partitionBy("zona").orderBy(F.col("num_viajes").desc())
    resultado = agregado.withColumn("rank_en_zona", F.row_number().over(ventana_zona))

    resultado.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(
        f"{gold_path}/agg_demanda_estacion"
    )
    print(f"[gold] Agg_Demanda_Estacion: {resultado.count()} filas "
          f"(join + groupBy + window)")


if __name__ == "__main__":
    main()
