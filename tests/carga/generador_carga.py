"""
Generador de carga para la prueba de escalabilidad (RNF-01)
===========================================================

Publica eventos sinteticos directamente en Kinesis a una tasa configurable,
para probar que el sistema soporta el pico de 330 eventos/s (10x la carga
normal) manteniendo la latencia de alertas < 1 min (RNF-01).

NOTA: este generador es SOLO para pruebas de carga. El sistema en produccion
usa el feed real de Citi Bike (ver streaming-ingestion/producer). Aqui los datos
son sinteticos porque necesitamos controlar la TASA exacta de eventos/s, algo
que el feed real (que se actualiza cada 60s) no permite.

Cómo se ejecuta (con credenciales de AWS configuradas):
  python generador_carga.py \
    --stream bikenexus-estaciones \
    --region us-east-1 \
    --eventos-por-segundo 330 \
    --duracion 120
"""

import argparse
import json
import time
import random
from datetime import datetime, timezone

import boto3


def generar_evento(estacion_id: int) -> dict:
    """Crea un evento sintetico con el mismo esquema que el productor real."""
    capacidad = random.choice([20, 25, 30, 35])
    bikes = random.randint(0, capacidad)
    return {
        "estacion_id": str(estacion_id),
        "bikes_disponibles": bikes,
        "docks_libres": capacidad - bikes,
        "capacidad": capacidad,
        "activa": True,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ingest_ts": datetime.now(timezone.utc).isoformat(),
    }


def main():
    parser = argparse.ArgumentParser(description="Generador de carga (RNF-01)")
    parser.add_argument("--stream", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--eventos-por-segundo", type=int, default=330)
    parser.add_argument("--duracion", type=int, default=120, help="segundos")
    parser.add_argument("--num-estaciones", type=int, default=2000)
    args = parser.parse_args()

    kinesis = boto3.client("kinesis", region_name=args.region)

    print(f"Enviando {args.eventos_por_segundo} ev/s durante {args.duracion}s "
          f"a {args.stream}...")

    total_enviados = 0
    total_fallidos = 0
    inicio = time.time()

    while time.time() - inicio < args.duracion:
        t_lote = time.time()

        # Armamos un lote de 'eventos_por_segundo' eventos (max 500 por PutRecords).
        pendientes = []
        for _ in range(args.eventos_por_segundo):
            est = random.randint(1, args.num_estaciones)
            e = generar_evento(est)
            pendientes.append({
                "Data": json.dumps(e).encode("utf-8"),
                "PartitionKey": e["estacion_id"],
            })

        # Enviamos en sublotes de 500 con reintento de fallidos (como el productor real).
        for i in range(0, len(pendientes), 500):
            sublote = pendientes[i:i + 500]
            resp = kinesis.put_records(StreamName=args.stream, Records=sublote)
            total_enviados += len(sublote)
            total_fallidos += resp["FailedRecordCount"]

        # Ajustamos para mantener ~1 segundo por iteracion.
        transcurrido = time.time() - t_lote
        if transcurrido < 1.0:
            time.sleep(1.0 - transcurrido)

    duracion_real = time.time() - inicio
    print(f"Enviados: {total_enviados} | Fallidos: {total_fallidos} | "
          f"Tasa real: {total_enviados / duracion_real:.0f} ev/s")
    if total_fallidos > 0:
        print("AVISO: hubo fallidos -> revisar throttling de Kinesis (añadir shards).")


if __name__ == "__main__":
    main()
