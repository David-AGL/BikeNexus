"""
Pruebas unitarias de la lógica del productor (RF-01, RNF-03)
===========================================================

Verifican, SIN AWS, que:
  - cada evento se publica con estacion_id como partition key (RF-01),
  - el reintento de registros fallidos funciona (RNF-03, at-least-once).

Se ejecutan con:  python -m pytest tests/test_logica_productor.py -v
"""

import json
import sys
import types
from pathlib import Path

# Hacemos importable el modulo del productor sin instalar nada.
PRODUCER_DIR = Path(__file__).resolve().parents[1] / "streaming-ingestion" / "producer"
sys.path.insert(0, str(PRODUCER_DIR))


# --- Stub de boto3: simula Kinesis sin conexion real -------------------------
class KinesisFake:
    """Simula un cliente de Kinesis que falla los N primeros registros."""

    def __init__(self, fallar_primeros=0):
        self.fallar_primeros = fallar_primeros
        self.recibidos = []        # todos los records que llegaron (con reintentos)
        self.llamadas = 0

    def put_records(self, StreamName, Records):  # noqa: N803 (firma de boto3)
        self.llamadas += 1
        self.recibidos.extend(Records)
        resultados = []
        fallidos = 0
        for i, _ in enumerate(Records):
            # En la primera llamada, fallan los primeros N; luego todo OK.
            if self.llamadas == 1 and i < self.fallar_primeros:
                resultados.append({"ErrorCode": "ProvisionedThroughputExceeded"})
                fallidos += 1
            else:
                resultados.append({"SequenceNumber": f"seq-{i}", "ShardId": "shard-0"})
        return {"FailedRecordCount": fallidos, "Records": resultados}


def _cargar_productor(kinesis_fake):
    """Importa lambda_producer inyectando un boto3 falso."""
    boto3_fake = types.ModuleType("boto3")
    boto3_fake.client = lambda nombre: kinesis_fake
    sys.modules["boto3"] = boto3_fake

    # Importacion limpia cada vez.
    if "lambda_producer" in sys.modules:
        del sys.modules["lambda_producer"]
    import lambda_producer  # noqa: E402
    return lambda_producer


def test_partition_key_es_estacion_id():
    """Cada record debe usar estacion_id como PartitionKey (RF-01)."""
    fake = KinesisFake()
    lp = _cargar_productor(fake)

    eventos = [
        {"estacion_id": "72", "bikes_disponibles": 5},
        {"estacion_id": "100", "bikes_disponibles": 0},
    ]
    records = lp._a_records_kinesis(eventos)

    assert records[0]["PartitionKey"] == "72"
    assert records[1]["PartitionKey"] == "100"
    # El payload debe ser el JSON del evento.
    assert json.loads(records[0]["Data"])["estacion_id"] == "72"


def test_reintento_de_fallidos():
    """Si Kinesis rechaza registros, deben reintentarse hasta publicarse (RNF-03)."""
    # La primera llamada falla 2 registros; la segunda ya no.
    fake = KinesisFake(fallar_primeros=2)
    lp = _cargar_productor(fake)

    eventos = [{"estacion_id": str(i)} for i in range(5)]
    records = lp._a_records_kinesis(eventos)

    sin_publicar = lp.publicar_lote_con_reintentos(records)

    assert sin_publicar == 0           # al final no queda ninguno sin publicar
    assert fake.llamadas == 2          # hizo falta un reintento
