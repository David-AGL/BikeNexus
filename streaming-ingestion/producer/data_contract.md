# Contrato de datos — evento de estado de estación

Este es el "contrato" que viaja por toda la tubería de streaming. Define qué campos
tiene cada evento desde que la Lambda lo lee de la API hasta que llega a Silver.

## Fuente: Citi Bike NY (estándar GBFS v2)

El estado de una estación se arma combinando DOS feeds, porque ninguno tiene todo:

### 1. `station_status.json` — estado en tiempo real (cambia cada ~60 s)

Estructura del feed:
```json
{
  "last_updated": 1730800000,
  "ttl": 5,
  "data": {
    "stations": [
      {
        "station_id": "72",
        "num_bikes_available": 5,
        "num_docks_available": 20,
        "is_installed": 1,
        "is_renting": 1,
        "is_returning": 1,
        "last_reported": 1730799990
      }
    ]
  }
}
```

### 2. `station_information.json` — datos fijos (cambia ~1 vez al día)

```json
{
  "data": {
    "stations": [
      {
        "station_id": "72",
        "name": "W 52 St & 11 Ave",
        "lat": 40.767,
        "lon": -73.993,
        "capacity": 25
      }
    ]
  }
}
```

De aquí sacamos la **capacidad**, que `station_status` NO trae y que necesitamos
para validar en Silver (RF-04: `bikes_disponibles > capacidad` es inválido).

## Evento que publica la Lambda en Kinesis

La Lambda une ambos feeds por `station_id` y publica este objeto normalizado:

```json
{
  "estacion_id": "72",
  "bikes_disponibles": 5,
  "docks_libres": 20,
  "capacidad": 25,
  "activa": true,
  "timestamp": "2026-11-05T10:30:00Z",
  "fuente_last_reported": 1730799990,
  "ingest_ts": "2026-11-05T10:30:01.123Z"
}
```

| Campo | Origen | Descripción |
|-------|--------|-------------|
| `estacion_id` | station_id | Identificador. Es la **partition key** en Kinesis (RF-01) |
| `bikes_disponibles` | num_bikes_available | Bicicletas disponibles |
| `docks_libres` | num_docks_available | Espacios libres para devolver |
| `capacidad` | capacity (station_information) | Capacidad total de la estación |
| `activa` | is_installed==1 && is_renting==1 | Si la estación está operativa |
| `timestamp` | last_reported (convertido a ISO 8601) | Hora del reporte. Parte de la clave de dedup (RNF-03) |
| `fuente_last_reported` | last_reported | Epoch original, para trazabilidad |
| `ingest_ts` | generado por la Lambda | Cuándo lo leyó la Lambda (para medir latencia RF-01 < 5 s) |

## Clave de deduplicación (RNF-03)

`(estacion_id, timestamp)` → en Silver no puede existir más de un registro con la
misma combinación. Esto convierte el "at-least-once" del productor en un efecto
"exactly-once" en Silver.

## Reglas de validez (RF-04)

Un evento es **inválido** (se aparta, no entra a Silver) si:
- `bikes_disponibles > capacidad` (imposible físicamente)
- `capacidad == 0` (estación mal configurada)
