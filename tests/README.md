# Pruebas — BikeNexus

Dos tipos de pruebas: **unitarias** (corren sin AWS) y **de sistema** (requieren
el despliegue en AWS Academy).

## 1. Pruebas unitarias (sin AWS)

Verifican la lógica crítica sin conexión. Se ejecutan en local:

```bash
pip install pytest
python -m pytest tests/ -v
```

| Archivo | Qué verifica | Requisito |
|---------|--------------|-----------|
| `test_logica_productor.py` | Partition key = estacion_id; reintento de fallidos | RF-01, RNF-03 |
| `test_reglas_calidad.py` | Validación de inválidos y clasificación | RF-04, RF-06 |

## 2. Pruebas de sistema (en AWS Academy)

Se ejecutan tras desplegar la infraestructura. Cubren los RNF del hito "Pruebas".

### RNF-01 — Carga (330 eventos/s)
Objetivo: soportar el pico de 10x (330 ev/s) con latencia < 1 min.
```bash
python tests/carga/generador_carga.py --stream bikenexus-estaciones \
  --region us-east-1 --eventos-por-segundo 330 --duracion 120
```
Verificar en CloudWatch: métrica `IncomingRecords` del stream ~= 330/s y
`WriteProvisionedThroughputExceeded` cercano a 0.

### RNF-05 — Tolerancia a fallos
1. **Reintento de registros rechazados**: el generador de carga fuerza throttling;
   verificar en los logs de la Lambda que `fallidos` vuelve a 0 tras reintentos.
2. **Reinicio desde checkpoint**: detener el job de Spark a mitad, reiniciarlo y
   comprobar que reanuda desde el último sequence number (sin perder ni duplicar).
   La deduplicación de Silver (RNF-03) elimina cualquier duplicado del reproceso.

### RNF-04 — Latencia
- **Streaming**: medir desde `ingest_ts` (cuando la Lambda leyó el dato) hasta que
  la alerta aparece en DynamoDB. Debe ser < 1 min.
- **Batch**: medir desde que el CSV llega a Landing hasta que Gold es consultable.
  Debe ser < 24 h.

Ver `tests/latencia/medir_latencia.py`.
