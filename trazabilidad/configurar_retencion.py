"""
Configurar la retención del historial de versiones (Time Travel) — RNF-06
=========================================================================

RNF-06 exige conservar el historial de versiones de cada tabla Delta por al
menos 30 dias. Este script aplica esa configuracion a todas las tablas del
Data Lake, de modo que se pueda hacer time travel hasta 30 dias atras.

Cómo se ejecuta (en EMR):
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    configurar_retencion.py \
    --data-bucket bikenexus-datalake-eafit
"""

import argparse

from pyspark.sql import SparkSession

# Tablas Delta del Data Lake a las que aplicar la retencion de 30 dias.
TABLAS = [
    "bronze/estaciones",
    "silver/estaciones",
    "bronze/viajes",
    "silver/viajes",
    "bronze/catalogo",
    "silver/catalogo",
    "gold/fact_viajes",
    "gold/dim_estacion",
    "gold/dim_tiempo",
]


def crear_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bikenexus-config-retencion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser(description="Configurar retencion Delta (RNF-06)")
    parser.add_argument("--data-bucket", required=True)
    args = parser.parse_args()

    spark = crear_spark()
    spark.sparkContext.setLogLevel("WARN")

    for tabla in TABLAS:
        ruta = f"s3://{args.data_bucket}/{tabla}"
        try:
            spark.sql(f"""
                ALTER TABLE delta.`{ruta}`
                SET TBLPROPERTIES (
                    'delta.logRetentionDuration' = 'interval 30 days',
                    'delta.deletedFileRetentionDuration' = 'interval 30 days'
                )
            """)
            print(f"[OK] Retencion 30 dias configurada: {ruta}")
        except Exception as e:  # noqa: BLE001
            # Si la tabla aun no existe (no se ha corrido su job), avisamos y seguimos.
            print(f"[SKIP] {ruta}: {e}")


if __name__ == "__main__":
    main()
