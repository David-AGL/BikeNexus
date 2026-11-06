# 🚲 BikeNexus — Sistema de datos para bicicletas compartidas

Sistema de datos distribuido para una red de bicicletas compartidas (dominio:
logística). Convierte datos de viajes y estado operativo en **alertas en tiempo
real** (para operadores) e **indicadores históricos** (para analistas).

Universidad EAFIT — Sistemas Intensivos de Datos.

## Arquitectura

Dos caminos sobre una arquitectura Medallion (Bronze/Silver/Gold) en AWS:

```
                          ┌─────────── CAMINO STREAMING (tiempo real) ───────────┐
API Citi Bike ─► Lambda ─► Kinesis ─► Spark Streaming ─► Bronze ─► Silver ─► Clasificación ─► DynamoDB ─► Dashboard
  (cada 60s)    productora  (2 shards)   (EMR)           (crudo)  (dedup)   (ventanas 15min)            (alertas)

                          ┌─────────── CAMINO BATCH (histórico) ─────────────────┐
CSV.zip viajes ─► Lambda ─► S3 Landing ─► Spark batch ─► Bronze ─► Silver ─► Gold (star schema) ─► Athena
  (mensual)      carga batch              (EMR)          (crudo)  (calidad)  (Fact + Dims)        (SQL analítico)
     │                         ▲
Catálogo (diario) ─────────────┘           Orquestado por Step Functions (mensual) + EventBridge
```

## Estructura del repositorio

```
├── streaming-ingestion/   # Lambda productora, Kinesis, Spark Bronze/Silver (RF-01, RF-04, RNF-03)
├── analytics-streaming/   # Ventanas, clasificación, DynamoDB, dashboard (RF-06, RF-07, RNF-02)
├── batch-ingestion/       # Lambda carga, Spark batch, Gold, Athena, Step Functions (RF-02,03,05,08)
├── infra/                 # Terraform: toda la infraestructura (RNF-01, RNF-05)
├── trazabilidad/          # Linaje, Delta time travel, metadatos (RNF-06)
├── tests/                 # Pruebas unitarias + de carga/latencia
└── README.md
```

## Cobertura de requisitos

| Req | Dónde | Estado |
|-----|-------|--------|
| RF-01 ingesta 60s → Kinesis | `streaming-ingestion/producer` | ✅ |
| RF-02 viajes → Bronze sin pérdida | `batch-ingestion/spark/batch_viajes.py` | ✅ |
| RF-03 catálogo → Bronze/Silver | `batch-ingestion/spark/batch_catalogo.py` | ✅ |
| RF-04 limpieza + dedup Silver | `streaming-ingestion/spark/silver_stream.py` | ✅ |
| RF-05 limpieza viajes | `batch-ingestion/spark/batch_viajes.py` | ✅ |
| RF-06 ventanas + clasificación | `analytics-streaming/spark/clasificacion_stream.py` | ✅ |
| RF-07 alertas DynamoDB + dashboard | `analytics-streaming/` | ✅ |
| RF-08 Gold star schema + Athena | `batch-ingestion/spark/batch_gold.py`, `athena/` | ✅ |
| RNF-01 escalabilidad (2 shards, 330 ev/s) | `infra/`, `tests/carga` | ✅ |
| RNF-02 disponibilidad (AP, desactualizadas) | `analytics-streaming/dashboard` | ✅ |
| RNF-03 at-least-once + exactly-once Silver | productor + `silver_stream.py` | ✅ |
| RNF-04 latencia < 1 min / batch < 24h | `tests/latencia` | ✅ |
| RNF-05 tolerancia a fallos (checkpoints) | jobs Spark + Kinesis 24h | ✅ |
| RNF-06 trazabilidad | `trazabilidad/` | ✅ |

## Despliegue en AWS Academy (paso a paso)

### Requisitos previos
- AWS CLI y Terraform instalados.
- Sesión de AWS Academy activa (credenciales en "AWS Details → AWS CLI").
- Python 3.11.

### 1. Configurar credenciales (cada sesión de Academy)
Copia las credenciales de AWS Academy a `~/.aws/credentials` (expiran al cerrar).

### 2. Desplegar la infraestructura
```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
# Edita terraform.tfvars: pega el ARN del LabRole (IAM → Roles → LabRole).
terraform init
terraform plan        # revisa lo que creará
terraform apply       # crea S3, Kinesis, DynamoDB, Lambdas, EMR, Step Functions, EventBridge
```
Anota los outputs (nombres de buckets, EMR cluster id, etc.).

### 3. Subir el código de Spark a S3
```bash
aws s3 cp streaming-ingestion/spark/ s3://<CODE_BUCKET>/spark/ --recursive
aws s3 cp batch-ingestion/spark/     s3://<CODE_BUCKET>/spark/ --recursive
```

### 4. Lanzar los jobs de streaming en EMR
```bash
# Bronze
aws emr add-steps --cluster-id <EMR_ID> --steps Type=Spark,Name=bronze,\
Args=[--packages,io.delta:delta-spark_2.12:3.1.0,s3://<CODE_BUCKET>/spark/bronze_stream.py,\
--stream,bikenexus-estaciones,--region,us-east-1,\
--bronze-path,s3://<DATA_BUCKET>/bronze/estaciones,\
--checkpoint-path,s3://<DATA_BUCKET>/checkpoints/bronze]
# (igual para silver_stream.py y clasificacion_stream.py)
```

### 5. Camino batch
El flujo batch lo dispara EventBridge (mensual) o lo lanzas manualmente iniciando
la máquina de estados de Step Functions desde la consola.

### 6. Dashboard
```bash
cd analytics-streaming/dashboard
pip install -r requirements.txt
export DYNAMODB_TABLA=bikenexus-estado-estaciones
streamlit run dashboard.py
```

### 7. Consultas analíticas (Athena)
Registra Gold con un crawler de Glue (ver `batch-ingestion/athena/03_crawler_glue.md`)
y ejecuta `batch-ingestion/athena/02_consultas_analiticas.sql`.

### 8. ⚠️ Apagar todo al terminar (ahorro de créditos)
```bash
# Terminar el clúster EMR (o con terraform destroy)
cd infra && terraform destroy
```

## Pruebas
```bash
pip install pytest
python -m pytest tests/ -v          # unitarias (sin AWS): 11 pruebas
# Carga y latencia: ver tests/README.md (requieren despliegue)
```

## Decisiones técnicas
Documentadas como ADR en `Entrega_1_Sistemas_Intensivos.pdf`:
Kinesis (ADR-01), Spark (ADR-02), Delta Lake (ADR-03), DynamoDB (ADR-04),
Step Functions + EventBridge (ADR-05).
