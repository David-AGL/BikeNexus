"""
Pipeline LOCAL — Kinesis -> Bronze -> Silver -> Clasificación -> DynamoDB
=========================================================================

Reproduce en Python puro la MISMA LOGICA de los jobs de Spark, para poder ver el
flujo completo del camino de streaming en local sin montar un cluster Spark.

Hace:
  1. Lee los eventos del stream de Kinesis (LocalStack).
  2. BRONZE: guarda el crudo en S3 local (LocalStack) como JSON (equivale a Delta).
  3. SILVER: deduplica por (estacion_id, timestamp) y aparta invalidos (RF-04).
  4. CLASIFICACION: clasifica cada estacion (VACIA/LLENA/EN_RIESGO/NORMAL) (RF-06).
  5. DynamoDB: escribe el ultimo estado de cada estacion (RF-07).

NOTA: la clasificacion de tendencia (RF-06) necesita varios reportes por estacion
en el tiempo; aqui usamos una version simplificada basada en el estado actual
(VACIA/LLENA y, si hay historico en Bronze, estimacion de tendencia). La logica de
ventana deslizante completa corre en el job de Spark real.

Uso:
  python run_pipeline_local.py
"""

import json
from decimal import Decimal
from datetime import datetime, timezone

import boto3

ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"
STREAM = "bikenexus-estaciones"
BUCKET = "bikenexus-datalake"
TABLA = "bikenexus-estado-estaciones"

import os
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", REGION)

kinesis = boto3.client("kinesis", endpoint_url=ENDPOINT, region_name=REGION)
s3 = boto3.client("s3", endpoint_url=ENDPOINT, region_name=REGION)
dynamodb = boto3.resource("dynamodb", endpoint_url=ENDPOINT, region_name=REGION)


# --- 1. Leer todos los eventos disponibles en Kinesis ------------------------
def leer_de_kinesis() -> list[dict]:
    """Lee todos los registros disponibles en todos los shards del stream."""
    desc = kinesis.describe_stream(StreamName=STREAM)
    eventos = []
    for shard in desc["StreamDescription"]["Shards"]:
        it = kinesis.get_shard_iterator(
            StreamName=STREAM,
            ShardId=shard["ShardId"],
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]

        # Leemos en varias pasadas hasta que no haya mas registros.
        for _ in range(5):
            resp = kinesis.get_records(ShardIterator=it, Limit=1000)
            for r in resp["Records"]:
                eventos.append(json.loads(r["Data"]))
            it = resp["NextShardIterator"]
            if not resp["Records"]:
                break
    return eventos


# --- 2. Bronze: guardar crudo en S3 ------------------------------------------
def guardar_bronze(eventos: list[dict]):
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    key = f"bronze/estaciones/{ts}.json"
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body="\n".join(json.dumps(e) for e in eventos).encode("utf-8"),
    )
    print(f"  BRONZE: {len(eventos)} registros guardados en s3://{BUCKET}/{key}")


# --- 3. Silver: dedup + validacion (RF-04, RNF-03) ---------------------------
def procesar_silver(eventos: list[dict]) -> tuple[list[dict], list[dict]]:
    # Dedup por (estacion_id, timestamp).
    vistos = {}
    for e in eventos:
        clave = (e["estacion_id"], e.get("timestamp"))
        vistos[clave] = e  # el ultimo gana (idempotente)
    unicos = list(vistos.values())

    validos, invalidos = [], []
    for e in unicos:
        cap = e.get("capacidad", 0) or 0
        bikes = e.get("bikes_disponibles", 0) or 0
        if cap <= 0 or bikes > cap:
            invalidos.append(e)
        else:
            validos.append(e)

    dup_eliminados = len(eventos) - len(unicos)
    print(f"  SILVER: {len(validos)} validos | {len(invalidos)} invalidos (cuarentena) "
          f"| {dup_eliminados} duplicados eliminados")
    return validos, invalidos


# --- 4. Clasificacion (RF-06, version por estado actual) ---------------------
# Umbral de riesgo: si se vaciaria/llenaria en menos de estos minutos -> EN_RIESGO (RF-06).
UMBRAL_RIESGO_MIN = 15

# Archivo donde guardamos el ultimo estado de cada estacion, para poder calcular
# la TENDENCIA (velocidad de cambio) entre un ciclo y el siguiente. Esto reproduce
# en local la ventana deslizante que en AWS hace Spark Structured Streaming (RF-06).
HISTORIAL_PATH = os.path.join(os.path.dirname(__file__), ".historial_estaciones.json")


def cargar_historial() -> dict:
    import json as _json
    if os.path.exists(HISTORIAL_PATH):
        try:
            with open(HISTORIAL_PATH, encoding="utf-8") as f:
                return _json.load(f)
        except (ValueError, OSError):
            return {}
    return {}


def guardar_historial(hist: dict):
    import json as _json
    with open(HISTORIAL_PATH, "w", encoding="utf-8") as f:
        _json.dump(hist, f)


def clasificar(e: dict, historial: dict, ahora_epoch: float) -> tuple[str, float | None]:
    """
    Clasifica una estacion usando la TENDENCIA (RF-06):
      - VACIA / LLENA: por estado actual (0 bicis / 0 docks).
      - EN_RIESGO: si, al ritmo al que cambia, se vaciaria o llenaria en < 15 min.
      - NORMAL: en cualquier otro caso.

    La tendencia se calcula comparando el estado actual con el guardado en el
    ciclo anterior (historial). Asi estimamos bicis/min y docks/min.
    """
    est_id = str(e["estacion_id"])
    bikes = e.get("bikes_disponibles", 0) or 0
    docks = e.get("docks_libres", 0) or 0

    # Estado actual ya en el limite.
    if bikes == 0:
        return "VACIA", 0.0
    if docks == 0:
        return "LLENA", 0.0

    # ¿Tenemos un reporte anterior de esta estacion para medir la tendencia?
    anterior = historial.get(est_id)
    if anterior:
        dt_min = (ahora_epoch - anterior["t"]) / 60.0
        if dt_min > 0:
            tasa_bikes = (bikes - anterior["bikes"]) / dt_min   # <0 = se vacia
            tasa_docks = (docks - anterior["docks"]) / dt_min   # <0 = se llena

            min_vaciarse = bikes / abs(tasa_bikes) if tasa_bikes < 0 else None
            min_llenarse = docks / abs(tasa_docks) if tasa_docks < 0 else None

            candidatos = [m for m in (min_vaciarse, min_llenarse) if m is not None]
            if candidatos:
                min_rest = min(candidatos)
                if min_rest < UMBRAL_RIESGO_MIN:
                    return "EN_RIESGO", round(min_rest, 1)

    return "NORMAL", None


# --- 5. Escribir en DynamoDB (RF-07) -----------------------------------------
def escribir_dynamodb(validos: list[dict]):
    tabla = dynamodb.Table(TABLA)
    ahora_dt = datetime.now(timezone.utc)
    ahora = ahora_dt.isoformat()
    ahora_epoch = ahora_dt.timestamp()

    # Cargamos el historial para calcular la tendencia (RF-06).
    historial = cargar_historial()
    nuevo_historial = {}

    n_alerta, n_riesgo = 0, 0
    with tabla.batch_writer() as batch:
        for e in validos:
            clasif, min_rest = clasificar(e, historial, ahora_epoch)
            if clasif in ("VACIA", "LLENA", "EN_RIESGO"):
                n_alerta += 1
            if clasif == "EN_RIESGO":
                n_riesgo += 1

            est_id = str(e["estacion_id"])
            item = {
                "estacion_id": est_id,
                "clasificacion": clasif,
                "min_estimado_restante": Decimal(str(min_rest)) if min_rest is not None else Decimal("999999"),
                "bikes_disponibles": int(e.get("bikes_disponibles", 0) or 0),
                "docks_libres": int(e.get("docks_libres", 0) or 0),
                "capacidad": int(e.get("capacidad", 0) or 0),
                "activa": bool(e.get("activa", True)),
                "ultimo_reporte": e.get("timestamp", ahora),
                "actualizado_en": ahora,
            }
            # Nombre y coordenadas (para el mapa del dashboard), si vienen.
            if e.get("nombre"):
                item["nombre"] = str(e["nombre"])
            if e.get("lat") is not None and e.get("lon") is not None:
                item["lat"] = Decimal(str(e["lat"]))
                item["lon"] = Decimal(str(e["lon"]))
            batch.put_item(Item=item)

            # Guardamos el estado actual para el proximo ciclo (tendencia).
            nuevo_historial[est_id] = {
                "t": ahora_epoch,
                "bikes": int(e.get("bikes_disponibles", 0) or 0),
                "docks": int(e.get("docks_libres", 0) or 0),
            }

    guardar_historial(nuevo_historial)
    print(f"  DYNAMODB: {len(validos)} estaciones escritas | {n_alerta} en alerta "
          f"(de ellas {n_riesgo} EN_RIESGO por tendencia)")


def main():
    print("=== Pipeline local: Kinesis -> Bronze -> Silver -> Clasificacion -> DynamoDB ===\n")
    eventos = leer_de_kinesis()
    print(f"  KINESIS: {len(eventos)} eventos leidos del stream")
    if not eventos:
        print("  No hay eventos. Corre primero run_producer_local.py")
        return

    guardar_bronze(eventos)
    validos, _invalidos = procesar_silver(eventos)
    escribir_dynamodb(validos)
    print("\n==> Listo. Abre el dashboard para visualizar las alertas.")


if __name__ == "__main__":
    main()
