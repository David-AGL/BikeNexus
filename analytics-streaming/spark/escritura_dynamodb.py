"""
Escritura del estado clasificado en DynamoDB (RF-07, RNF-02, ADR-04)
====================================================================

Qué hace:
  Toma el DataFrame de estaciones ya clasificadas (salida del Paso 5) y escribe
  el ULTIMO estado de cada estacion en DynamoDB mediante UPSERT (put_item).

Por qué upsert (put_item) y no insert:
  put_item sobrescribe la fila si ya existe esa estacion_id. Asi la escritura
  es IDEMPOTENTE: aunque un micro-batch se reprocese tras un fallo, el resultado
  final es el mismo (solo queda el estado mas reciente). DynamoDB guarda unica-
  mente el dato actual; el historico vive en Delta Lake (ADR-04).

Como lo consume el dashboard (RF-07):
  El GSI "gsi-clasificacion-tiempo" permite consultar, por ejemplo:
     "dame las estaciones EN_RIESGO ordenadas por min_estimado_restante"
  sin escanear toda la tabla.

Como se usa:
  Esta funcion se llama desde el job de clasificacion (foreachBatch), o puede
  ejecutarse de forma independiente leyendo la tabla de clasificacion.
"""

import os
from decimal import Decimal
from datetime import datetime, timezone

import boto3

TABLA = os.environ.get("DYNAMODB_TABLA", "bikenexus-estado-estaciones")
REGION = os.environ.get("AWS_REGION", "us-east-1")


def _a_decimal(valor):
    """DynamoDB no acepta float: convertimos a Decimal (o None si es nulo)."""
    if valor is None:
        return None
    return Decimal(str(round(float(valor), 4)))


def escribir_particion(filas):
    """
    Escribe un grupo de filas (una particion de Spark) en DynamoDB.
    Se ejecuta en los executors, por eso crea su propio cliente de boto3.
    Usa batch_writer, que agrupa y reintenta automaticamente los no procesados.
    """
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    tabla = dynamodb.Table(TABLA)
    ahora = datetime.now(timezone.utc).isoformat()

    with tabla.batch_writer() as batch:
        for fila in filas:
            # min_estimado_restante puede ser None (estacion NORMAL sin riesgo).
            # El GSI necesita un numero, asi que ponemos un valor alto (999999)
            # para que esas estaciones queden al final del orden por urgencia.
            min_restante = _a_decimal(fila["min_estimado_restante"])
            if min_restante is None:
                min_restante = Decimal("999999")

            item = {
                "estacion_id": str(fila["estacion_id"]),
                "clasificacion": fila["clasificacion"],
                "min_estimado_restante": min_restante,
                "bikes_disponibles": int(fila["bikes_fin"] or 0),
                "docks_libres": int(fila["docks_fin"] or 0),
                "capacidad": int(fila["capacidad"] or 0),
                "activa": bool(fila["activa"]),
                "ventana_fin": str(fila["ventana_fin"]),
                # Hora del ultimo reporte: el dashboard la usa para marcar
                # estaciones "desactualizadas" si no reportan en > 2 min (RNF-02).
                "ultimo_reporte": str(fila["ventana_fin"]),
                "actualizado_en": ahora,
            }
            pct = _a_decimal(fila["pct_ocupacion"])
            if pct is not None:
                item["pct_ocupacion"] = pct

            batch.put_item(Item=item)


def escribir_estado_en_dynamodb(df):
    """
    Recorre el DataFrame clasificado y escribe cada estacion en DynamoDB.
    foreachPartition reparte el trabajo entre los executors de Spark.
    """
    columnas = [
        "estacion_id", "clasificacion", "min_estimado_restante",
        "bikes_fin", "docks_fin", "capacidad", "activa",
        "ventana_fin", "pct_ocupacion",
    ]
    # Convertimos cada fila a dict para manejarla facilmente en el executor.
    (
        df.select(*columnas)
        .rdd
        .map(lambda r: r.asDict())
        .foreachPartition(escribir_particion)
    )
