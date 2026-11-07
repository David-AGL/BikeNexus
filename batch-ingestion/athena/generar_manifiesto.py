"""
Generar el manifiesto de Delta para que Athena lea la capa Gold (RF-08)
=======================================================================

El problema:
  Las tablas de Gold estan en formato Delta Lake. Athena NO entiende el
  _delta_log: si se registra la carpeta como Parquet plano, Athena ve TODOS
  los archivos .parquet (incluidas versiones viejas) o ninguno, y la consulta
  sale vacia o con datos duplicados.

La solucion (manifiesto symlink):
  Delta puede generar un 'symlink_format_manifest': un archivo de texto que
  lista EXACTAMENTE los .parquet vigentes de la tabla. Athena lee ese manifiesto
  con el formato SymlinkTextInputFormat y asi ve solo los datos correctos.

  Ademas activamos 'delta.compatibility.symlinkFormatManifest.enabled=true' en
  cada tabla, para que el manifiesto se regenere solo en cada escritura futura.

Que hace este script:
  Para cada tabla Delta de Gold en S3, llama a DeltaTable.generate(...) que crea
  la carpeta '_symlink_format_manifest/' dentro de la tabla. Despues, el DDL de
  Athena (registrar_tablas_manifiesto.sql) apunta a esa carpeta.

Como se ejecuta (desde tu PC, con credenciales de AWS Academy activas):
  pip install -r batch-ingestion/spark/requirements-batch-local.txt
  python batch-ingestion/athena/generar_manifiesto.py \
      --gold-s3 s3a://bikenexus-datalake-athina2026/gold

  (s3a:// es el esquema que usa el conector de Hadoop; el bucket es el mismo.)
"""

import argparse
import os
import sys

# Reutiliza la configuracion de Hadoop para Windows (winutils) del runner batch.
AQUI = os.path.dirname(os.path.abspath(__file__))
SPARK_DIR = os.path.abspath(os.path.join(AQUI, "..", "spark"))
if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

# Importa el helper que configura HADOOP_HOME en Windows antes de arrancar Spark.
import run_local as _rl  # noqa: E402  (ejecuta _configurar_hadoop_windows al importar)

from pyspark.sql import SparkSession  # noqa: E402
from delta import configure_spark_with_delta_pip  # noqa: E402
from delta.tables import DeltaTable  # noqa: E402

# Tablas de Gold a las que generamos manifiesto.
TABLAS_GOLD = ["agg_demanda_estacion", "dim_estacion", "dim_tiempo", "fact_viajes"]

# Versiones de los conectores. hadoop-aws DEBE coincidir con la version de
# Hadoop que empaqueta PySpark 3.5.1 (Hadoop 3.3.4).
PAQUETES = ",".join([
    "io.delta:delta-spark_2.12:3.1.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
])


def crear_spark() -> SparkSession:
    """
    Spark local que puede LEER/ESCRIBIR en S3 real usando las credenciales de
    AWS Academy (incluido el token de sesion, que Academy exige).
    """
    builder = (
        SparkSession.builder
        .appName("bikenexus-generar-manifiesto")
        .master("local[*]")
        .config("spark.jars.packages", PAQUETES)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Credenciales de AWS: usa la cadena de proveedores por defecto, que
        # incluye variables de entorno, ~/.aws/credentials y el token de sesion.
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    parser = argparse.ArgumentParser(description="Generar manifiesto Delta para Athena")
    parser.add_argument(
        "--gold-s3",
        default="s3a://bikenexus-datalake-athina2026/gold",
        help="Ruta s3a:// de la capa Gold (sin barra final)",
    )
    args = parser.parse_args()
    gold = args.gold_s3.rstrip("/")

    # Aviso temprano si no hay credenciales en el entorno (Academy las rota).
    tiene_token = bool(os.environ.get("AWS_SESSION_TOKEN"))
    if not (os.environ.get("AWS_ACCESS_KEY_ID") or os.path.exists(
            os.path.join(os.path.expanduser("~"), ".aws", "credentials"))):
        print("[WARN] No veo credenciales de AWS. Exporta AWS_ACCESS_KEY_ID / "
              "AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN o configura ~/.aws/credentials.")
    if not tiene_token:
        print("[nota] AWS_SESSION_TOKEN no esta en el entorno; si usas el Lab de "
              "Academy, suele ser necesario. Si tu ~/.aws/credentials ya lo tiene, ignora esto.")

    spark = crear_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print(" Generando manifiesto Delta para Athena")
    print(" Gold:", gold)
    print("=" * 70)

    for tabla in TABLAS_GOLD:
        ruta = f"{gold}/{tabla}"
        print(f"\n-> {tabla}")
        try:
            dt = DeltaTable.forPath(spark, ruta)
            # 1) Activar regeneracion automatica del manifiesto en cada escritura.
            spark.sql(
                f"ALTER TABLE delta.`{ruta}` "
                f"SET TBLPROPERTIES("
                f"delta.compatibility.symlinkFormatManifest.enabled=true)"
            )
            # 2) Generar el manifiesto ahora mismo.
            dt.generate("symlink_format_manifest")
            filas = dt.toDF().count()
            print(f"   OK manifiesto generado · {filas} filas vigentes")
        except Exception as e:  # noqa: BLE001
            print(f"   [ERROR] no se pudo generar manifiesto para {tabla}: {e}")

    print("\nListo. Ahora registra las tablas en Athena con:")
    print("   batch-ingestion/athena/registrar_tablas_manifiesto.sql")
    spark.stop()


if __name__ == "__main__":
    main()
