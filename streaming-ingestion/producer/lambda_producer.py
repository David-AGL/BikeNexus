"""
Lambda productora — BikeNexus (camino de streaming)
===================================================

Qué hace (RF-01, RNF-03):
  1. EventBridge la dispara cada 60 segundos.
  2. Consulta la API de Citi Bike NY (station_status + station_information).
  3. Une ambos feeds por station_id y arma un evento normalizado por estación.
  4. Publica los eventos en Kinesis usando estacion_id como PARTITION KEY
     (así los reportes de una misma estación quedan en el mismo shard y en orden).
  5. Garantía AT-LEAST-ONCE: revisa FailedRecordCount en cada PutRecords y
     REINTENTA los registros rechazados (nunca perdemos un dato).

Variables de entorno esperadas:
  - KINESIS_STREAM_NAME : nombre del stream de Kinesis (ej. "bikenexus-estaciones")
  - AWS_REGION          : región (la pone AWS automáticamente)
  - MAX_REINTENTOS      : nº máximo de reintentos de PutRecords (default 5)
"""

import json
import os
import time
import logging
from datetime import datetime, timezone
from urllib.request import urlopen, Request

import boto3

# --- Configuración / logging --------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

STREAM_NAME = os.environ.get("KINESIS_STREAM_NAME", "bikenexus-estaciones")
MAX_REINTENTOS = int(os.environ.get("MAX_REINTENTOS", "5"))

# URLs del feed GBFS de Citi Bike NY
URL_STATUS = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
URL_INFO = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"

# Kinesis solo acepta hasta 500 registros por llamada a PutRecords.
TAMANO_LOTE = 500

# Cliente de Kinesis (se crea una vez y se reutiliza entre invocaciones).
kinesis = boto3.client("kinesis")


# --- 1. Leer la API -----------------------------------------------------------
def _descargar_json(url: str) -> dict:
    """Descarga un feed GBFS y lo devuelve como diccionario."""
    req = Request(url, headers={"User-Agent": "BikeNexus/1.0"})
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def obtener_estado_estaciones() -> list[dict]:
    """
    Descarga los dos feeds, los une por station_id y devuelve una lista de
    eventos normalizados (según el contrato de datos).
    """
    status = _descargar_json(URL_STATUS)
    info = _descargar_json(URL_INFO)

    # Los datos fijos (capacidad, nombre, ubicacion) viven en
    # station_information -> los indexamos por station_id para buscarlos rápido
    # mientras recorremos station_status.
    info_por_estacion = {
        s["station_id"]: {
            "capacidad": s.get("capacity", 0),
            "nombre": s.get("name", ""),
            "lat": s.get("lat"),
            "lon": s.get("lon"),
        }
        for s in info["data"]["stations"]
    }

    ingest_ts = datetime.now(timezone.utc).isoformat()
    eventos = []

    for s in status["data"]["stations"]:
        estacion_id = s["station_id"]
        last_reported = s.get("last_reported")
        info_est = info_por_estacion.get(estacion_id, {})

        evento = {
            "estacion_id": estacion_id,
            "bikes_disponibles": s.get("num_bikes_available", 0),
            "docks_libres": s.get("num_docks_available", 0),
            "capacidad": info_est.get("capacidad", 0),
            "nombre": info_est.get("nombre", ""),
            "lat": info_est.get("lat"),
            "lon": info_est.get("lon"),
            "activa": bool(s.get("is_installed", 0)) and bool(s.get("is_renting", 0)),
            # last_reported viene en epoch (segundos); lo pasamos a ISO 8601.
            "timestamp": (
                datetime.fromtimestamp(last_reported, tz=timezone.utc).isoformat()
                if last_reported else ingest_ts
            ),
            "fuente_last_reported": last_reported,
            "ingest_ts": ingest_ts,
        }
        eventos.append(evento)

    return eventos


# --- 2. Publicar en Kinesis con reintentos (AT-LEAST-ONCE, RNF-03) -------------
def _a_records_kinesis(eventos: list[dict]) -> list[dict]:
    """Convierte cada evento al formato que espera PutRecords."""
    return [
        {
            "Data": json.dumps(evento).encode("utf-8"),
            # ESTA es la pieza clave del RF-01: la partition key es estacion_id,
            # para que los reportes de la misma estación vayan al mismo shard.
            "PartitionKey": str(evento["estacion_id"]),
        }
        for evento in eventos
    ]


def publicar_lote_con_reintentos(records: list[dict]) -> int:
    """
    Publica un lote de hasta 500 records en Kinesis.
    Si Kinesis rechaza algunos (FailedRecordCount > 0), los REINTENTA con
    backoff exponencial. Devuelve cuántos quedaron sin publicar tras agotar
    los reintentos (idealmente 0).
    """
    pendientes = records
    intento = 0

    while pendientes and intento <= MAX_REINTENTOS:
        if intento > 0:
            # Backoff exponencial: 0.1s, 0.2s, 0.4s, ... para no saturar Kinesis.
            espera = 0.1 * (2 ** (intento - 1))
            logger.warning("Reintento %d: %d registros pendientes (espera %.2fs)",
                           intento, len(pendientes), espera)
            time.sleep(espera)

        resp = kinesis.put_records(StreamName=STREAM_NAME, Records=pendientes)

        # Si no falló ninguno, terminamos.
        if resp["FailedRecordCount"] == 0:
            return 0

        # Si fallaron algunos, nos quedamos SOLO con los que fallaron para
        # reintentarlos (los que llevan "ErrorCode" en la respuesta).
        nuevos_pendientes = []
        for record, resultado in zip(pendientes, resp["Records"]):
            if resultado.get("ErrorCode"):
                nuevos_pendientes.append(record)
        pendientes = nuevos_pendientes
        intento += 1

    if pendientes:
        logger.error("Quedaron %d registros sin publicar tras %d reintentos",
                     len(pendientes), MAX_REINTENTOS)
    return len(pendientes)


# --- 3. Handler (punto de entrada que invoca AWS) -----------------------------
def handler(event, context):
    """Punto de entrada de la Lambda. EventBridge lo llama cada 60 s."""
    t0 = time.time()

    eventos = obtener_estado_estaciones()
    logger.info("Obtenidas %d estaciones de la API", len(eventos))

    records = _a_records_kinesis(eventos)

    total_fallidos = 0
    # Publicamos en lotes de 500 (límite de PutRecords).
    for i in range(0, len(records), TAMANO_LOTE):
        lote = records[i:i + TAMANO_LOTE]
        total_fallidos += publicar_lote_con_reintentos(lote)

    duracion = time.time() - t0
    resultado = {
        "estaciones_leidas": len(eventos),
        "publicados_ok": len(eventos) - total_fallidos,
        "fallidos": total_fallidos,
        "duracion_s": round(duracion, 3),
    }
    logger.info("Resultado: %s", json.dumps(resultado))

    # RF-01 exige latencia < 5 s. Si nos pasamos, lo dejamos registrado.
    if duracion > 5:
        logger.warning("Latencia %.2fs supera el objetivo de 5s (RF-01)", duracion)

    return resultado
