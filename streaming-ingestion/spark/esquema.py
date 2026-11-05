"""
Esquema del evento de estado de estación.
==========================================

Define la estructura del JSON que la Lambda publica en Kinesis (ver
producer/data_contract.md). Spark necesita este esquema para parsear el
JSON que llega en el campo "Data" de cada registro de Kinesis.

Tener el esquema en un solo archivo evita duplicarlo entre el job de Bronze
y el de Silver, y facilita mantenerlo.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    LongType,
)

# Esquema del evento normalizado que produce la Lambda.
ESQUEMA_ESTACION = StructType([
    StructField("estacion_id", StringType(), nullable=False),
    StructField("bikes_disponibles", IntegerType(), nullable=True),
    StructField("docks_libres", IntegerType(), nullable=True),
    StructField("capacidad", IntegerType(), nullable=True),
    StructField("activa", BooleanType(), nullable=True),
    StructField("timestamp", StringType(), nullable=True),          # ISO 8601
    StructField("fuente_last_reported", LongType(), nullable=True), # epoch original
    StructField("ingest_ts", StringType(), nullable=True),          # cuándo lo leyó la Lambda
])
