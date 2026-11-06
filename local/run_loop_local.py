"""
Bucle LOCAL automático — imita a EventBridge + Spark Streaming de AWS
====================================================================

Ejecuta en bucle, cada N segundos:
   1. productor  -> consulta el feed real de Citi Bike y publica en Kinesis
   2. pipeline   -> Kinesis -> Bronze -> Silver -> clasificacion -> DynamoDB

Así el dashboard siempre tiene datos frescos, igual que en AWS (donde EventBridge
dispara el productor cada 60s y Spark Streaming procesa continuamente).

Déjalo corriendo en una ventana y abre el dashboard en otra. Para detener: Ctrl+C.

Uso:
  python run_loop_local.py                 # ciclo cada 60s (como AWS)
  python run_loop_local.py --intervalo 30  # ciclo cada 30s
"""

import time
import argparse
import importlib
from datetime import datetime

# Importamos los dos modulos locales para reutilizar su logica.
productor = importlib.import_module("run_producer_local")
pipeline = importlib.import_module("run_pipeline_local")


def un_ciclo():
    """Ejecuta un ciclo completo: traer datos + procesarlos."""
    # 1. Productor: 1 consulta al feed real -> Kinesis.
    #    Reutilizamos la logica del productor (1 solo ciclo).
    eventos = productor.lp.obtener_estado_estaciones()
    records = productor.lp._a_records_kinesis(eventos)
    fallidos = 0
    for j in range(0, len(records), productor.lp.TAMANO_LOTE):
        lote = records[j:j + productor.lp.TAMANO_LOTE]
        fallidos += productor.lp.publicar_lote_con_reintentos(lote)
    print(f"    productor: {len(eventos)} estaciones -> Kinesis (fallidos: {fallidos})")

    # 2. Pipeline: Kinesis -> Bronze -> Silver -> DynamoDB.
    eventos_kinesis = pipeline.leer_de_kinesis()
    if not eventos_kinesis:
        print("    pipeline: sin eventos nuevos")
        return
    pipeline.guardar_bronze(eventos_kinesis)
    validos, _ = pipeline.procesar_silver(eventos_kinesis)
    pipeline.escribir_dynamodb(validos)


def main():
    parser = argparse.ArgumentParser(description="Bucle local automatico (imita AWS)")
    parser.add_argument("--intervalo", type=int, default=60,
                        help="segundos entre ciclos (default 60, como EventBridge)")
    args = parser.parse_args()

    print("=== Bucle local automatico (Ctrl+C para detener) ===")
    print(f"    Un ciclo cada {args.intervalo}s. Abre el dashboard en otra ventana.\n")

    ciclo = 0
    try:
        while True:
            ciclo += 1
            hora = datetime.now().strftime("%H:%M:%S")
            print(f"[{hora}] Ciclo {ciclo}:")
            try:
                un_ciclo()
            except Exception as e:  # noqa: BLE001
                print(f"    ERROR en el ciclo: {e}")
            print(f"    (esperando {args.intervalo}s...)\n")
            time.sleep(args.intervalo)
    except KeyboardInterrupt:
        print("\n==> Bucle detenido. ¡Hasta luego!")


if __name__ == "__main__":
    main()
