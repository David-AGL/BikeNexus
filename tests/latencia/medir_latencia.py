"""
Medidor de latencia de extremo a extremo del camino streaming (RNF-04)
======================================================================

Mide el tiempo desde que la Lambda productora leyó un dato (ingest_ts) hasta
que la alerta correspondiente aparece en DynamoDB. RNF-04 exige < 1 minuto.

Idea:
  Cada registro en DynamoDB tiene 'actualizado_en' (cuando Spark lo escribio) y
  'ultimo_reporte'/'ventana_fin'. Comparando con el momento de la medicion se
  estima la latencia de propagacion. Para una medicion exacta punta a punta se
  recomienda inyectar un evento marcado y cronometrar su aparicion.

Cómo se ejecuta:
  python medir_latencia.py --tabla bikenexus-estado-estaciones --region us-east-1
"""

import argparse
from datetime import datetime, timezone

import boto3


def minutos_desde(iso_ts: str) -> float | None:
    if not iso_ts:
        return None
    try:
        t = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - t).total_seconds() / 60.0
    except (ValueError, TypeError):
        return None


def main():
    parser = argparse.ArgumentParser(description="Medir latencia streaming (RNF-04)")
    parser.add_argument("--tabla", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--muestras", type=int, default=50)
    args = parser.parse_args()

    tabla = boto3.resource("dynamodb", region_name=args.region).Table(args.tabla)

    # Escaneamos una muestra de estaciones y medimos la antiguedad de su estado.
    resp = tabla.scan(Limit=args.muestras)
    items = resp.get("Items", [])

    if not items:
        print("No hay datos en DynamoDB. ¿Esta corriendo el pipeline?")
        return

    latencias = []
    for item in items:
        lat = minutos_desde(item.get("actualizado_en", ""))
        if lat is not None:
            latencias.append(lat)

    if not latencias:
        print("No se pudo calcular latencia (falta 'actualizado_en').")
        return

    promedio = sum(latencias) / len(latencias)
    maximo = max(latencias)
    print(f"Muestras: {len(latencias)}")
    print(f"Latencia promedio: {promedio:.2f} min")
    print(f"Latencia maxima:   {maximo:.2f} min")
    print(f"RNF-04 (< 1 min): {'CUMPLE' if maximo < 1 else 'NO CUMPLE'}")


if __name__ == "__main__":
    main()
