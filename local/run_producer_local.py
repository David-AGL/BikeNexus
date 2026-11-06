"""
Productor LOCAL — consulta el feed real de Citi Bike y publica en Kinesis (LocalStack)
======================================================================================

Es la misma logica de la Lambda productora (streaming-ingestion/producer), pero
preparada para correr en tu PC contra LocalStack. Consulta el feed GBFS REAL de
Citi Bike NY y publica los eventos en el stream local.

Uso:
  python run_producer_local.py --ciclos 5 --intervalo 10

  --ciclos    : cuantas veces consulta el feed (default 3)
  --intervalo : segundos entre consultas (default 10)
"""

import os
import sys
import time
import json
import argparse
from pathlib import Path

import boto3

# Reutilizamos la logica real del productor (no duplicamos codigo).
PRODUCER_DIR = Path(__file__).resolve().parents[1] / "streaming-ingestion" / "producer"
sys.path.insert(0, str(PRODUCER_DIR))
import lambda_producer as lp  # noqa: E402

# Apuntamos boto3 a LocalStack en vez de a AWS real.
ENDPOINT = "http://localhost:4566"
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

# Sobrescribimos el cliente de Kinesis del productor para que use LocalStack.
lp.kinesis = boto3.client("kinesis", endpoint_url=ENDPOINT, region_name="us-east-1")
lp.STREAM_NAME = "bikenexus-estaciones"


def main():
    parser = argparse.ArgumentParser(description="Productor local contra LocalStack")
    parser.add_argument("--ciclos", type=int, default=3)
    parser.add_argument("--intervalo", type=int, default=10)
    args = parser.parse_args()

    for i in range(1, args.ciclos + 1):
        print(f"\n=== Ciclo {i}/{args.ciclos}: consultando feed real de Citi Bike ===")
        try:
            eventos = lp.obtener_estado_estaciones()
            print(f"  Estaciones obtenidas del feed: {len(eventos)}")
            records = lp._a_records_kinesis(eventos)

            fallidos = 0
            for j in range(0, len(records), lp.TAMANO_LOTE):
                lote = records[j:j + lp.TAMANO_LOTE]
                fallidos += lp.publicar_lote_con_reintentos(lote)
            print(f"  Publicados en Kinesis: {len(eventos) - fallidos} | fallidos: {fallidos}")
            # Muestra un evento de ejemplo para que veas el dato real.
            if eventos:
                print(f"  Ejemplo: {json.dumps(eventos[0], ensure_ascii=False)}")
        except Exception as e:  # noqa: BLE001
            print(f"  ERROR consultando/publicando: {e}")
            print("  (¿Tienes internet? ¿LocalStack está arriba? ¿Corriste setup_local.sh?)")

        if i < args.ciclos:
            print(f"  Esperando {args.intervalo}s hasta el próximo ciclo...")
            time.sleep(args.intervalo)

    print("\n==> Productor local finalizado. Los datos están en Kinesis (LocalStack).")


if __name__ == "__main__":
    main()
