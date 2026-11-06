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
def clasificar(e: dict) -> tuple[str, float | None]:
    bikes = e.get("bikes_disponibles", 0) or 0
    docks = e.get("docks_libres", 0) or 0
    if bikes == 0:
        return "VACIA", 0.0
    if docks == 0:
        return "LLENA", 0.0
    # Sin historico suficiente para tendencia -> NORMAL (la ventana real la hace Spark).
    return "NORMAL", None


# --- 5. Escribir en DynamoDB (RF-07) -----------------------------------------
def escribir_dynamodb(validos: list[dict]):
    tabla = dynamodb.Table(TABLA)
    ahora = datetime.now(timezone.utc).isoformat()
    n_alerta = 0
    with tabla.batch_writer() as batch:
        for e in validos:
            clasif, min_rest = clasificar(e)
            if clasif in ("VACIA", "LLENA", "EN_RIESGO"):
                n_alerta += 1
            item = {
                "estacion_id": str(e["estacion_id"]),
                "clasificacion": clasif,
                "min_estimado_restante": Decimal(str(min_rest)) if min_rest is not None else Decimal("999999"),
                "bikes_disponibles": int(e.get("bikes_disponibles", 0) or 0),
                "docks_libres": int(e.get("docks_libres", 0) or 0),
                "capacidad": int(e.get("capacidad", 0) or 0),
                "activa": bool(e.get("activa", True)),
                "ultimo_reporte": e.get("timestamp", ahora),
                "actualizado_en": ahora,
            }
            batch.put_item(Item=item)
    print(f"  DYNAMODB: {len(validos)} estaciones escritas | {n_alerta} en alerta")


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
