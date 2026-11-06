"""
Spark Structured Streaming: Silver -> Clasificación de estaciones
=================================================================

Qué hace (RF-06):
  Sobre los datos LIMPIOS de Silver, calcula para cada estación:
    1. El porcentaje de ocupación.
    2. La TENDENCIA de cambio usando una VENTANA DESLIZANTE de 15 minutos con
       avance de 5 minutos (sliding window).
    3. Clasifica cada estación en una de cuatro categorías:
         - VACIA     : 0 bicicletas disponibles.
         - LLENA     : 0 espacios libres (docks).
         - EN_RIESGO : al ritmo actual quedaria vacia o llena en < 15 min.
         - NORMAL    : cualquier otro caso.

Idea de la "tendencia":
  Dentro de cada ventana de 15 min comparamos el PRIMER y el ULTIMO reporte de
  la estacion. Si las bicis bajan, calculamos a que ritmo (bicis por minuto) y
  estimamos cuantos minutos faltan para llegar a 0 (vaciarse). Lo mismo con los
  docks para estimar cuanto falta para llenarse. Si ese tiempo estimado es
  menor a 15 min, la estacion esta EN_RIESGO.

Salida:
  Escribe el ultimo estado clasificado de cada estacion. Este resultado lo
  consumira la etapa de alertas (DynamoDB) y el dashboard.

Cómo se ejecuta (en EMR):
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    clasificacion_stream.py \
    --silver-path s3://bikenexus-datalake/silver/estaciones \
    --salida-path s3://bikenexus-datalake/clasificacion/estaciones \
    --checkpoint-path s3://bikenexus-datalake/checkpoints/clasificacion
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

# La escritura a DynamoDB es opcional: si boto3 no esta disponible (p. ej. en
# pruebas locales sin AWS), el job sigue funcionando y solo escribe a Delta.
try:
    from escritura_dynamodb import escribir_estado_en_dynamodb
except ImportError:
    escribir_estado_en_dynamodb = None

# Parámetros de la ventana deslizante (RF-06).
VENTANA_MIN = 15          # tamaño de la ventana
AVANCE_MIN = 5            # cada cuánto avanza (slide)
UMBRAL_RIESGO_MIN = 15    # si se vaciaría/llenaría en menos de esto -> EN_RIESGO


def crear_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bikenexus-clasificacion-stream")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def calcular_clasificacion(batch_df, salida_path: str):
    """
    Se ejecuta por cada micro-batch (foreachBatch).
    Recibe los registros nuevos de Silver, agrupa por ventana deslizante y
    estacion, calcula la tendencia y clasifica cada estacion.
    """
    spark = batch_df.sparkSession

    # El timestamp viene como string ISO; lo convertimos a tipo timestamp
    # para poder hacer ventanas temporales y ordenar.
    df = batch_df.withColumn("ts", F.to_timestamp("timestamp"))

    # 1. Agrupamos en ventanas deslizantes de 15 min con avance de 5 min,
    #    por estacion. Dentro de cada (ventana, estacion) recogemos:
    #    - el estado mas reciente (ultimo reporte),
    #    - el primero y el ultimo valor de bicis/docks para medir la tendencia.
    agrupado = (
        df.groupBy(
            F.window("ts", f"{VENTANA_MIN} minutes", f"{AVANCE_MIN} minutes"),
            F.col("estacion_id"),
        )
        .agg(
            F.max("ts").alias("ts_fin"),
            F.min("ts").alias("ts_inicio"),
            # Valores al inicio y al fin de la ventana (por orden temporal).
            F.first("bikes_disponibles", ignorenulls=True).alias("bikes_inicio"),
            F.last("bikes_disponibles", ignorenulls=True).alias("bikes_fin"),
            F.first("docks_libres", ignorenulls=True).alias("docks_inicio"),
            F.last("docks_libres", ignorenulls=True).alias("docks_fin"),
            F.last("capacidad", ignorenulls=True).alias("capacidad"),
            F.last("activa", ignorenulls=True).alias("activa"),
        )
    )

    # 2. Calculamos minutos transcurridos dentro de la ventana y las tasas
    #    de cambio (bicis por minuto y docks por minuto).
    con_tasas = (
        agrupado
        .withColumn(
            "minutos_ventana",
            (F.col("ts_fin").cast("long") - F.col("ts_inicio").cast("long")) / 60.0,
        )
        # Ocupacion = bicis / capacidad (0..1). Protegemos division por cero.
        .withColumn(
            "pct_ocupacion",
            F.when(F.col("capacidad") > 0,
                   F.col("bikes_fin") / F.col("capacidad")).otherwise(F.lit(None)),
        )
        # Tasa de cambio de bicis por minuto (negativa = se esta vaciando).
        .withColumn(
            "tasa_bikes",
            F.when(F.col("minutos_ventana") > 0,
                   (F.col("bikes_fin") - F.col("bikes_inicio")) / F.col("minutos_ventana"))
             .otherwise(F.lit(0.0)),
        )
        # Tasa de cambio de docks por minuto (negativa = se esta llenando).
        .withColumn(
            "tasa_docks",
            F.when(F.col("minutos_ventana") > 0,
                   (F.col("docks_fin") - F.col("docks_inicio")) / F.col("minutos_ventana"))
             .otherwise(F.lit(0.0)),
        )
    )

    # 3. Estimamos minutos restantes para vaciarse o llenarse.
    #    - Si las bicis bajan (tasa_bikes < 0), tiempo = bikes_fin / |tasa_bikes|.
    #    - Si los docks bajan (tasa_docks < 0), tiempo = docks_fin / |tasa_docks|.
    con_estimaciones = (
        con_tasas
        .withColumn(
            "min_para_vaciarse",
            F.when(F.col("tasa_bikes") < 0,
                   F.col("bikes_fin") / F.abs(F.col("tasa_bikes")))
             .otherwise(F.lit(None)),
        )
        .withColumn(
            "min_para_llenarse",
            F.when(F.col("tasa_docks") < 0,
                   F.col("docks_fin") / F.abs(F.col("tasa_docks")))
             .otherwise(F.lit(None)),
        )
    )

    # 4. Clasificacion segun RF-06 (orden de prioridad: vacia/llena primero).
    clasificado = (
        con_estimaciones
        .withColumn(
            "clasificacion",
            F.when(F.col("bikes_fin") == 0, F.lit("VACIA"))
             .when(F.col("docks_fin") == 0, F.lit("LLENA"))
             .when(
                 ((F.col("min_para_vaciarse").isNotNull()) &
                  (F.col("min_para_vaciarse") < UMBRAL_RIESGO_MIN)) |
                 ((F.col("min_para_llenarse").isNotNull()) &
                  (F.col("min_para_llenarse") < UMBRAL_RIESGO_MIN)),
                 F.lit("EN_RIESGO"),
             )
             .otherwise(F.lit("NORMAL")),
        )
        # Tiempo estimado restante (el menor de los dos, si aplica). Lo usara
        # el dashboard para ordenar las estaciones por urgencia (RF-07).
        .withColumn(
            "min_estimado_restante",
            F.least(F.col("min_para_vaciarse"), F.col("min_para_llenarse")),
        )
    )

    # 5. Como una estacion puede caer en varias ventanas deslizantes a la vez,
    #    nos quedamos con la ventana MAS RECIENTE de cada estacion (su estado actual).
    w = Window.partitionBy("estacion_id").orderBy(F.col("window.end").desc())
    actual = (
        clasificado
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            "estacion_id",
            F.col("window.end").alias("ventana_fin"),
            "capacidad",
            "activa",
            "bikes_fin",
            "docks_fin",
            "pct_ocupacion",
            "tasa_bikes",
            "tasa_docks",
            "min_estimado_restante",
            "clasificacion",
        )
    )

    # 6. Escribimos el estado clasificado en Delta (historico/trazabilidad).
    (
        actual.write
        .format("delta")
        .mode("overwrite")  # siempre el estado mas reciente de cada estacion
        .save(salida_path)
    )

    # 7. Escribimos el estado en DynamoDB para el dashboard (RF-07).
    #    Es un upsert idempotente: solo queda el ultimo estado de cada estacion.
    if escribir_estado_en_dynamodb is not None:
        escribir_estado_en_dynamodb(actual)


def main():
    parser = argparse.ArgumentParser(description="Spark: Silver -> Clasificacion")
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--salida-path", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--trigger-segundos", type=int, default=30)
    args = parser.parse_args()

    spark = crear_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Leemos Silver como stream. Usamos watermark de 20 min para controlar
    # eventos que lleguen tarde sin acumular estado infinito (RNF-04).
    silver_stream = (
        spark.readStream
        .format("delta")
        .load(args.silver_path)
        .withColumn("ts", F.to_timestamp("timestamp"))
        .withWatermark("ts", "20 minutes")
    )

    query = (
        silver_stream.writeStream
        .foreachBatch(lambda df, bid: calcular_clasificacion(df, args.salida_path))
        .option("checkpointLocation", args.checkpoint_path)
        .trigger(processingTime=f"{args.trigger_segundos} seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
