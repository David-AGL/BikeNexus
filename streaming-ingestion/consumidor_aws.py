"""
Consumidor de streaming en AWS real (Kinesis -> Silver -> clasificacion -> DynamoDB)
====================================================================================

Lee los reportes desde Kinesis con boto3 (no necesita el conector de Spark, que
da problemas en EMR), aplica la MISMA logica del pipeline:
   - dedup por (estacion_id, timestamp)         (RNF-03)
   - aparta invalidos: bikes>capacidad, cap<=0  (RF-04)
   - clasifica VACIA/LLENA/EN_RIESGO/NORMAL con tendencia entre ciclos (RF-06)
   - escribe el ultimo estado en DynamoDB        (RF-07)

Corre en un bucle continuo (como haria Spark Streaming). Se puede ejecutar desde
tu PC (apuntando a AWS real con las credenciales de Academy) o desde EMR.

NOTA: usa el feed real de Citi Bike que ya publica la Lambda en Kinesis. Este
consumidor es el que procesa ese stream en AWS real.

Uso (con credenciales de AWS Academy activas):
  python consumidor_aws.py --intervalo 60

  --stream           nombre del stream de Kinesis (default: bikenexus-estaciones)
  --region           region AWS (default: us-east-1)
  --dynamodb-tabla   tabla DynamoDB (default: bikenexus-estado-estaciones)
  --intervalo        segundos entre ciclos de lectura (default: 60)
"""

import json
import time
import argparse
from decimal import Decimal
from datetime import datetime, timezone

import boto3

UMBRAL_RIESGO_MIN = 15

# Historial en memoria para calcular tendencia entre ciclos (RF-06).
_historial = {}


def iteradores_iniciales(kinesis, stream):
    """
    Crea un iterador por shard apuntando a TRIM_HORIZON (inicio de lo disponible,
    hasta 24h atras). Se llama UNA vez; luego se avanzan con NextShardIterator.
    """
    desc = kinesis.describe_stream(StreamName=stream)
    iteradores = {}
    for shard in desc["StreamDescription"]["Shards"]:
        sid = shard["ShardId"]
        iteradores[sid] = kinesis.get_shard_iterator(
            StreamName=stream, ShardId=sid, ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]
    return iteradores


def leer_kinesis(kinesis, iteradores):
    """
    Lee lo disponible desde la posicion actual de cada shard y AVANZA el iterador
    (no reinicia). Asi captura todo lo publicado sin releer ni perder datos.
    Modifica 'iteradores' en el sitio y devuelve los eventos leidos.
    """
    eventos = []
    for sid, it in list(iteradores.items()):
        if not it:
            continue
        # Hasta 5 pasadas por shard para drenar el backlog.
        for _ in range(5):
            resp = kinesis.get_records(ShardIterator=it, Limit=1000)
            for r in resp["Records"]:
                try:
                    eventos.append(json.loads(r["Data"]))
                except (ValueError, KeyError):
                    pass
            it = resp.get("NextShardIterator")
            iteradores[sid] = it
            if not it or not resp["Records"]:
                break
    return eventos


def procesar(eventos):
    """Dedup + validacion + clasificacion. Devuelve (validos, stats)."""
    # Dedup por (estacion_id, timestamp).
    vistos = {}
    for e in eventos:
        vistos[(e.get("estacion_id"), e.get("timestamp"))] = e
    unicos = list(vistos.values())

    ahora_epoch = datetime.now(timezone.utc).timestamp()
    validos, invalidos, n_riesgo = [], 0, 0

    for e in unicos:
        cap = e.get("capacidad", 0) or 0
        bikes = e.get("bikes_disponibles", 0) or 0
        docks = e.get("docks_libres", 0) or 0
        if cap <= 0 or bikes > cap:
            invalidos += 1
            continue

        # Clasificacion con tendencia (RF-06).
        clasif, min_rest = "NORMAL", None
        if bikes == 0:
            clasif, min_rest = "VACIA", 0.0
        elif docks == 0:
            clasif, min_rest = "LLENA", 0.0
        else:
            prev = _historial.get(e["estacion_id"])
            if prev:
                dt = (ahora_epoch - prev["t"]) / 60.0
                if dt > 0:
                    tb = (bikes - prev["bikes"]) / dt
                    td = (docks - prev["docks"]) / dt
                    mv = bikes / abs(tb) if tb < 0 else None
                    ml = docks / abs(td) if td < 0 else None
                    cand = [m for m in (mv, ml) if m is not None]
                    if cand and min(cand) < UMBRAL_RIESGO_MIN:
                        clasif, min_rest = "EN_RIESGO", round(min(cand), 1)
        if clasif == "EN_RIESGO":
            n_riesgo += 1

        e["_clasif"] = clasif
        e["_min_rest"] = min_rest
        validos.append(e)

        _historial[e["estacion_id"]] = {"t": ahora_epoch, "bikes": bikes, "docks": docks}

    return validos, {"unicos": len(unicos), "invalidos": invalidos, "riesgo": n_riesgo}


def escribir_dynamodb(tabla, validos):
    ahora = datetime.now(timezone.utc).isoformat()
    with tabla.batch_writer() as batch:
        for e in validos:
            mr = e["_min_rest"]
            item = {
                "estacion_id": str(e["estacion_id"]),
                "clasificacion": e["_clasif"],
                "min_estimado_restante": Decimal(str(mr)) if mr is not None else Decimal("999999"),
                "bikes_disponibles": int(e.get("bikes_disponibles", 0) or 0),
                "docks_libres": int(e.get("docks_libres", 0) or 0),
                "capacidad": int(e.get("capacidad", 0) or 0),
                "activa": bool(e.get("activa", True)),
                "ultimo_reporte": e.get("timestamp", ahora),
                "actualizado_en": ahora,
            }
            if e.get("nombre"):
                item["nombre"] = str(e["nombre"])
            if e.get("lat") is not None and e.get("lon") is not None:
                item["lat"] = Decimal(str(e["lat"]))
                item["lon"] = Decimal(str(e["lon"]))
            batch.put_item(Item=item)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream", default="bikenexus-estaciones")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--dynamodb-tabla", default="bikenexus-estado-estaciones")
    parser.add_argument("--intervalo", type=int, default=60)
    args = parser.parse_args()

    kinesis = boto3.client("kinesis", region_name=args.region)
    tabla = boto3.resource("dynamodb", region_name=args.region).Table(args.dynamodb_tabla)

    print("=== Consumidor AWS: Kinesis -> Silver -> clasificacion -> DynamoDB ===")
    print(f"    Stream: {args.stream} | Tabla: {args.dynamodb_tabla} | cada {args.intervalo}s")
    print("    (Ctrl+C para detener)\n")

    # Creamos los iteradores UNA vez (TRIM_HORIZON) y los vamos avanzando.
    iteradores = iteradores_iniciales(kinesis, args.stream)

    ciclo = 0
    try:
        while True:
            ciclo += 1
            hora = datetime.now().strftime("%H:%M:%S")
            eventos = leer_kinesis(kinesis, iteradores)
            if eventos:
                validos, stats = procesar(eventos)
                escribir_dynamodb(tabla, validos)
                en_alerta = sum(1 for e in validos if e["_clasif"] in ("VACIA", "LLENA", "EN_RIESGO"))
                print(f"[{hora}] Ciclo {ciclo}: {len(eventos)} leidos de Kinesis | "
                      f"{stats['unicos']} unicos | {stats['invalidos']} invalidos | "
                      f"{len(validos)} -> DynamoDB | {en_alerta} en alerta "
                      f"({stats['riesgo']} EN_RIESGO)")
            else:
                print(f"[{hora}] Ciclo {ciclo}: sin datos nuevos en Kinesis "
                      f"(¿la Lambda esta publicando?)")
            time.sleep(args.intervalo)
    except KeyboardInterrupt:
        print("\n==> Consumidor detenido.")


if __name__ == "__main__":
    main()
