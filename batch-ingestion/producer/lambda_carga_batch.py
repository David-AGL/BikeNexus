"""
Lambda de carga batch — BikeNexus (RF-02, RF-03)
================================================

Qué hace:
  Descarga datos de las fuentes y los deja CRUDOS en S3 Landing (sin transformar).
  Tiene DOS modos segun quien la invoque (lo indica el campo "modo" del evento):

  - modo = "viajes"   (RF-02): descarga el CSV.zip MENSUAL del historico de
    viajes, lo DESCOMPRIME y deja el CSV resultante en S3 Landing.
    La invoca Step Functions una vez al mes.

  - modo = "catalogo" (RF-03): descarga el catalogo DIARIO de estaciones (JSON)
    con id, nombre, ubicacion, capacidad y zona, y lo deja en S3 Landing.
    La invoca EventBridge una vez al dia.

Por que separar descarga (aqui) y procesamiento (Spark):
  Landing conserva el dato tal como lo entrego la fuente. Si el procesamiento
  falla, siempre se puede reprocesar desde Landing sin volver a descargar.

Variables de entorno:
  - LANDING_BUCKET    : bucket de S3 Landing (ej. "bikenexus-datalake")
  - URL_VIAJES_ZIP    : URL del CSV.zip mensual de viajes
  - URL_CATALOGO      : URL del catalogo de estaciones (station_information)
"""

import io
import os
import json
import zipfile
import logging
from datetime import datetime, timezone
from urllib.request import urlopen, Request

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

LANDING_BUCKET = os.environ.get("LANDING_BUCKET", "bikenexus-datalake")
URL_VIAJES_ZIP = os.environ.get(
    "URL_VIAJES_ZIP",
    "https://s3.amazonaws.com/tripdata/JC-202401-citibike-tripdata.csv.zip",
)
URL_CATALOGO = os.environ.get(
    "URL_CATALOGO",
    "https://gbfs.citibikenyc.com/gbfs/en/station_information.json",
)

s3 = boto3.client("s3")


def _descargar_bytes(url: str) -> bytes:
    """Descarga el contenido de una URL como bytes."""
    req = Request(url, headers={"User-Agent": "BikeNexus/1.0"})
    with urlopen(req, timeout=120) as resp:
        return resp.read()


# --- Modo viajes (RF-02) ------------------------------------------------------
def cargar_viajes() -> dict:
    """
    Descarga el CSV.zip de viajes, lo descomprime y sube cada CSV a S3 Landing.
    Devuelve un resumen de los archivos cargados.
    """
    logger.info("Descargando CSV.zip de viajes: %s", URL_VIAJES_ZIP)
    datos_zip = _descargar_bytes(URL_VIAJES_ZIP)

    # Mes de referencia para organizar la carpeta en Landing.
    mes = datetime.now(timezone.utc).strftime("%Y-%m")
    subidos = []

    # Descomprimimos en memoria y subimos cada CSV interno.
    with zipfile.ZipFile(io.BytesIO(datos_zip)) as zf:
        for nombre in zf.namelist():
            # Ignoramos carpetas internas y archivos de macOS (__MACOSX).
            if nombre.endswith("/") or nombre.startswith("__MACOSX"):
                continue
            if not nombre.lower().endswith(".csv"):
                continue

            contenido = zf.read(nombre)
            # Ruta en Landing: landing/viajes/AAAA-MM/archivo.csv
            key = f"landing/viajes/{mes}/{os.path.basename(nombre)}"
            s3.put_object(Bucket=LANDING_BUCKET, Key=key, Body=contenido)
            subidos.append(key)
            logger.info("Subido a s3://%s/%s (%d bytes)",
                        LANDING_BUCKET, key, len(contenido))

    return {"modo": "viajes", "mes": mes, "archivos": subidos}


# --- Modo catalogo (RF-03) ----------------------------------------------------
def cargar_catalogo() -> dict:
    """
    Descarga el catalogo de estaciones (JSON) y lo sube a S3 Landing.
    Se guarda tal cual lo entrega la fuente (crudo).
    """
    logger.info("Descargando catalogo de estaciones: %s", URL_CATALOGO)
    datos = _descargar_bytes(URL_CATALOGO)

    # Validamos que sea JSON parseable (si no, fallamos temprano).
    json.loads(datos.decode("utf-8"))

    dia = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"landing/catalogo/{dia}/station_information.json"
    s3.put_object(Bucket=LANDING_BUCKET, Key=key, Body=datos)
    logger.info("Subido a s3://%s/%s (%d bytes)", LANDING_BUCKET, key, len(datos))

    return {"modo": "catalogo", "dia": dia, "archivo": key}


# --- Handler ------------------------------------------------------------------
def handler(event, context):
    """
    Punto de entrada. El campo 'modo' del evento decide que cargar:
      {"modo": "viajes"}    -> carga el CSV.zip mensual (RF-02)
      {"modo": "catalogo"}  -> carga el catalogo diario  (RF-03)
    Por defecto usa "catalogo".
    """
    modo = (event or {}).get("modo", "catalogo")
    logger.info("Lambda de carga batch, modo=%s", modo)

    if modo == "viajes":
        resultado = cargar_viajes()
    elif modo == "catalogo":
        resultado = cargar_catalogo()
    else:
        raise ValueError(f"Modo desconocido: {modo}. Usa 'viajes' o 'catalogo'.")

    logger.info("Resultado: %s", json.dumps(resultado))
    return resultado
