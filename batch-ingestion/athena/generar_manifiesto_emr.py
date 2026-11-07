"""
Generar el manifiesto de Delta para Athena, EJECUTADO EN EMR (RF-08)
====================================================================

Igual que generar_manifiesto.py, pero pensado para correr como un STEP de EMR
con spark-submit --packages io.delta:delta-spark_2.12:3.1.0. En EMR el acceso a
S3 es nativo (esquema s3://, sin configurar S3A ni credenciales a mano), por lo
que es mas robusto que hacerlo desde un PC Windows.

Para cada tabla Delta de Gold en S3:
  1) ALTER TABLE ... SET TBLPROPERTIES(
        delta.compatibility.symlinkFormatManifest.enabled=true)
     -> el manifiesto se regenera solo en cada escritura futura.
  2) DeltaTable.generate("symlink_format_manifest")
     -> crea la carpeta '_symlink_format_manifest/' con la lista de .parquet
        vigentes, que Athena leera via SymlinkTextInputFormat.

Como se lanza (step de EMR):
  aws emr add-steps --cluster-id <EMR_ID> --steps \
    "Type=Spark,Name=manifiesto,Args=[--deploy-mode,cluster,--packages,\
     io.delta:delta-spark_2.12:3.1.0,\
     s3://<CODE>/spark/generar_manifiesto_emr.py,\
     --gold-path,s3://<DATA>/gold]"
"""

import argparse

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

TABLAS_GOLD = ["agg_demanda_estacion", "dim_estacion", "dim_tiempo", "fact_viajes"]


def crear_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bikenexus-generar-manifiesto-emr")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold-path", required=True,
                        help="Ruta s3:// de la capa Gold (sin barra final)")
    args = parser.parse_args()
    gold = args.gold_path.rstrip("/")

    spark = crear_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[manifiesto] Gold: {gold}")
    for tabla in TABLAS_GOLD:
        ruta = f"{gold}/{tabla}"
        print(f"[manifiesto] -> {tabla}")
        dt = DeltaTable.forPath(spark, ruta)
        spark.sql(
            f"ALTER TABLE delta.`{ruta}` "
            f"SET TBLPROPERTIES("
            f"delta.compatibility.symlinkFormatManifest.enabled=true)"
        )
        dt.generate("symlink_format_manifest")
        print(f"[manifiesto]    OK · {dt.toDF().count()} filas vigentes")

    print("[manifiesto] Listo. Registra las tablas en Athena con "
          "registrar_tablas_manifiesto.sql")
    spark.stop()


if __name__ == "__main__":
    main()
