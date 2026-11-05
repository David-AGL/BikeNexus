# Ingesta en streaming — BikeNexus

Componente del camino de **streaming** del proyecto BikeNexus.

Responsable: **Mariamny** · Revisor: **Emmanuel**
Requisitos cubiertos: **RF-01, RF-04, RNF-03**

## Qué hace este componente

```
API Citi Bike NY ──► Lambda productora ──► Kinesis Data Streams ──► Spark Structured Streaming ──► S3 Bronze ──► S3 Silver
   (station_status    (EventBridge cada 60s,   (2 shards,             (en EMR, micro-batches        (crudo,        (dedup +
    + station_info)    at-least-once)           key=estacion_id)       cada 30s)                     inmutable)     limpieza)
```

| Paso | Componente | Carpeta | Requisito |
|------|------------|---------|-----------|
| 1 | Lambda productora | `producer/` | RF-01, RNF-03 |
| 2 | Kinesis Data Streams | `infra/` | RF-01, RNF-03 |
| 3 | Spark → Bronze | `spark/` | RNF-03 |
| 4 | Spark → Silver (dedup + limpieza) | `spark/` | RF-04, RNF-03 |

## Estructura

```
streaming-ingestion/
├── producer/          # Lambda que consulta la API y publica en Kinesis
├── spark/             # Job de Spark Structured Streaming (Bronze + Silver)
├── infra/             # Definición de Kinesis, S3 y EventBridge
├── scripts/           # Utilidades para desplegar y probar
└── tests/             # Pruebas
```

## Fuente de datos (Citi Bike NY — GBFS)

- Estado en tiempo real: https://gbfs.citibikenyc.com/gbfs/en/station_status.json
- Información fija de estaciones (capacidad, nombre): https://gbfs.citibikenyc.com/gbfs/en/station_information.json

Se necesitan **ambos** feeds: `station_status` trae bicicletas/docks disponibles, y
`station_information` trae la **capacidad**, necesaria para validar en Silver (RF-04).
