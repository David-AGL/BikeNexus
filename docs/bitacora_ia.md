# Bitácora de delegación de agentes de IA — BikeNexus

> Documento obligatorio según la política de uso de IA del proyecto final (sin
> bitácora se descuenta el 10% del E2). Registra, de forma honesta y detallada,
> **qué hizo el agente de IA** y **qué decidió / validó el equipo** durante toda
> la implementación. El equipo entiende y puede defender todo el código entregado.

---

## 1. Agente utilizado y modo de trabajo

- **Agente:** Kiro (asistente de IA de desarrollo, entorno web).
- **Modo:** interactivo y guiado. El equipo definió las decisiones de arquitectura
  en el Entregable 1 (ADRs) y usó el agente para **generar el código** a partir de
  esas decisiones, **resolver problemas técnicos** del despliegue y **ajustar** la
  presentación de resultados.
- **Principio rector:** el agente escribe; el equipo decide, revisa y entiende.
  Ninguna decisión de arquitectura se delegó al agente — todas vienen del E1.

---

## 2. Qué DECIDIÓ el equipo (no delegado)

Estas decisiones son del equipo y están justificadas con ADRs en el E1. El agente
solo las implementó:

| Decisión | Justificación (ADR del E1) |
|----------|----------------------------|
| Kinesis como motor de streaming (vs Kafka/MSK) | ADR-01: servicio administrado, integración nativa AWS, replicación 3 AZ |
| Garantía at-least-once productor→Kinesis→Bronze | ADR-01 / RNF-03 |
| Spark Structured Streaming (vs Flink / batch puro) | ADR-02: ventanas, estado, mismo motor para batch y streaming |
| Delta Lake + arquitectura Medallion | ADR-03: transacciones ACID, time travel, Bronze/Silver/Gold |
| Modelo dimensional (star schema) en Gold | ADR-03: consultas analíticas rápidas |
| DynamoDB como estado operacional (componente libre) | ADR-04: baja latencia por clave, postura AP (CAP) |
| Step Functions + EventBridge como orquestador | ADR-05: administrado, sin servidores, integración Lambda/EMR |
| Clave de dedup = (estacion_id, timestamp) | RNF-03: efecto exactly-once en Silver |
| Partition key de Kinesis = estacion_id | RF-01: orden por estación dentro del shard |

---

## 3. Qué GENERÓ el agente (por componente)

### Camino de streaming
| Archivo | Qué generó el agente | Qué validó el equipo |
|---------|----------------------|----------------------|
| `streaming-ingestion/producer/lambda_producer.py` | Lambda que consulta GBFS de Citi Bike y publica en Kinesis con reintentos de `FailedRecordCount` | El contrato de datos, la partition key y la garantía at-least-once |
| `streaming-ingestion/spark/bronze_stream.py` | Lectura de Kinesis → Bronze crudo + metadatos de origen + checkpoints | Que Bronze es inmutable y conserva el linaje (RNF-06) |
| `streaming-ingestion/spark/silver_stream.py` | Dedup por (estacion_id, timestamp) vía MERGE + cuarentena de inválidos | Las reglas de calidad (RF-04) y el efecto exactly-once (RNF-03) |
| `analytics-streaming/spark/clasificacion_stream.py` | Ventana deslizante 15min/5min, tendencia y clasificación | Los umbrales y la lógica de riesgo (RF-06) |
| `analytics-streaming/spark/escritura_dynamodb.py` | Upsert idempotente a DynamoDB | El GSI (clasificación+tiempo) y la idempotencia |
| `analytics-streaming/dashboard/dashboard.py` | Dashboard Streamlit/Plotly con KPIs, dona, top 10 y tabla | Qué mostrar y la marca de "desactualizada" (RNF-02) |

### Camino batch
| Archivo | Qué generó el agente | Qué validó el equipo |
|---------|----------------------|----------------------|
| `batch-ingestion/producer/lambda_carga_batch.py` | Lambda que descarga/descomprime CSV.zip y catálogo a S3 Landing | Separación ingesta/transformación (RF-02, RF-03) |
| `batch-ingestion/spark/batch_catalogo.py` | Catálogo → Bronze → Silver (va antes que viajes) | El orden (RF-03) |
| `batch-ingestion/spark/batch_viajes.py` | Viajes → Bronze sin pérdida (assert) → Silver con join contra catálogo | Reglas RF-02 y RF-05, validación de estaciones |
| `batch-ingestion/spark/batch_gold.py` | Star schema + tabla agregada (join + groupBy + window) | El modelo dimensional (RF-08) y las preguntas de negocio |
| `batch-ingestion/athena/*.sql` | DDL de tablas Gold y consultas analíticas | Las preguntas de negocio del E1 que responden |

### Infraestructura y transversal
| Archivo | Qué generó el agente | Qué validó el equipo |
|---------|----------------------|----------------------|
| `infra/*.tf` (Terraform) | IaC de S3, Kinesis, DynamoDB, Lambdas, EMR, Step Functions, EventBridge | El stack del E1 y las restricciones de Academy |
| `trazabilidad/*` | Time travel Delta, metadatos, consultas de linaje | Los 3 mecanismos de RNF-06 |
| `tests/*` | 11 pruebas unitarias + scripts de carga/latencia | Que validan la lógica crítica (RF-01, RF-04, RF-06, RNF-03) |
| `local/*` | Entorno LocalStack para probar sin créditos | Que replica la arquitectura AWS |

---

## 4. Problemas técnicos resueltos durante la implementación

El equipo entiende la causa y la solución de cada uno (material clave para la
defensa). Cada fix quedó documentado en un Pull Request.

| # | Problema | Causa | Solución | PR |
|---|----------|-------|----------|----|
| 1 | Conector GitHub sin escritura | App no autorizada en el repo | David autorizó el repo | — |
| 2 | SCP de Academy bloquea object-lock de S3 | El proveedor AWS lee object-lock al crear/refrescar buckets | Buckets creados con AWS CLI, no con Terraform | #12, #13, #15 |
| 3 | Proveedor AWS 5.x incompatible | Lectura automática de object-lock | Fijar proveedor 4.67 | #13 |
| 4 | Runtime python3.11 no soportado por el proveedor 4.67 | Versión del proveedor | Cambiar Lambdas a python3.10 | #14 |
| 5 | SCP bloquea IncreaseStreamRetentionPeriod de Kinesis | El proveedor fija 24h al crear el stream | Crear Kinesis con AWS CLI, referenciar por variable | #26 |
| 6 | EMR: LabRole sin permisos EC2 | El service_role necesita permisos de EC2 | Usar EMR_DefaultRole / EMR_EC2_DefaultRole | #16 |
| 7 | Conector Spark-Kinesis falla en EMR | El conector es de terceros, no resuelve en EMR | Consumidor con boto3 (misma lógica) para la demo en vivo | #19, #20, #21 |
| 8 | Dashboard no auto-refrescaba | `st.autorefresh` no es nativo de Streamlit | Paquete `streamlit-autorefresh` + respaldo JS | #22, #23 |
| 9 | `plotly.express.scatter_mapbox` no existe | Plotly reciente renombró a `scatter_map` | Soporte de ambas versiones | #17 |
| 10 | Gold vacío en Athena | Viajes de Jersey City vs catálogo de NYC: el join no coincide | Generar el catálogo desde el propio CSV de viajes | #27, #28, #29 |

---

## 5. Resultados reales obtenidos (medidos)

- **Ingesta (RF-01):** la Lambda procesa **2518 estaciones reales cada 60s**, con
  **0 fallos** y **~1.5s de latencia** (muy por debajo del objetivo de 5s). Verificado
  en los logs de CloudWatch en AWS real.
- **Kinesis:** stream con **2 shards** activos (RNF-01).
- **Silver (RF-04, RNF-03):** en una corrida típica, de ~7000 lecturas acumuladas
  quedaron ~2488 estaciones únicas en DynamoDB (dedup funcionando) y ~30 registros
  apartados a cuarentena (validación funcionando).
- **Spark batch:** los jobs de catálogo, viajes y Gold corrieron a estado
  **COMPLETED** en EMR.
- **Pruebas unitarias:** 11/11 pasan (`pytest tests/ -v`).

---

## 6. Qué NO se delegó al agente

- La **definición del problema de negocio** y las preguntas analíticas (E1).
- Las **decisiones de arquitectura** y sus garantías (CAP, entrega, modelo dimensional).
- La **interpretación de la rúbrica** y qué priorizar.
- La **ejecución del despliegue** en AWS Academy (credenciales y comandos ejecutados
  por el equipo).

---

## 7. Buenas prácticas de uso del agente aplicadas

- Se pidió al agente generar **esqueletos y lógica concreta** ("implementa el
  productor que publica en Kinesis con reintentos"), no "diseña mi arquitectura".
- Cada componente se revisó y se probó (sintaxis, pruebas unitarias, ejecución en AWS).
- Los problemas se documentaron como PRs con explicación de causa y solución, de modo
  que cualquier integrante puede explicar por qué el código quedó así.

---

## 8. Verificación de comprensión del equipo

Cada integrante puede explicar y defender:
- El flujo de datos extremo a extremo (streaming y batch).
- Por qué Kinesis y no Kafka (ADR-01).
- Cómo se logra el efecto exactly-once en Silver (dedup + checkpoints, RNF-03).
- Por qué DynamoDB es AP y qué se sacrifica (ADR-04, RNF-02).
- Cómo el star schema responde las preguntas de negocio (RF-08).
- Las restricciones de AWS Academy encontradas y cómo se resolvieron.
