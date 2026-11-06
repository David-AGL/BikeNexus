# 🖥️ Probar BikeNexus en LOCAL (sin gastar créditos de AWS)

Esta guía te permite ver funcionar el **camino de streaming completo** en tu PC:
feed real de Citi Bike → Kinesis → Bronze → Silver → clasificación → DynamoDB →
**dashboard visual**. Todo con LocalStack (un "AWS falso" gratis).

## Requisitos (ya los tienes)
- Docker
- Python 3.11
- Internet (para consultar el feed real de Citi Bike)

---

## Paso a paso

### 1. Instalar las dependencias de Python
Desde la carpeta raíz del proyecto:
```bash
cd local
pip install -r requirements-local.txt
```

### 2. Levantar LocalStack (el "AWS falso")
```bash
docker compose up -d
```
Espera ~15 segundos a que arranque. Verifica que esté corriendo:
```bash
docker ps
```
Debes ver el contenedor `bikenexus-localstack`.

### 3. Crear los recursos (Kinesis, S3, DynamoDB)

**En Windows (PowerShell) — recomendado:**
```powershell
.\setup_local.ps1
```
Si PowerShell bloquea el script, corre primero:
`Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`

**En Linux / Mac / Git Bash:**
```bash
bash setup_local.sh
```

Debe imprimir que creó el stream, el bucket y la tabla, y al final listar
`bikenexus-estaciones`. Si NO aparece el stream, revisa la sección de problemas.

### 4. Publicar datos reales de Citi Bike en Kinesis
```bash
python run_producer_local.py --ciclos 3 --intervalo 5
```
Esto consulta el **feed real** 3 veces y mete los datos al Kinesis local.
Verás cuántas estaciones obtuvo y un ejemplo de evento real.

### 5. Procesar el pipeline (Bronze → Silver → DynamoDB)
```bash
python run_pipeline_local.py
```
Verás el conteo en cada etapa: eventos leídos, guardados en Bronze, válidos en
Silver (con duplicados eliminados) y escritos en DynamoDB.

### 6. 🎉 Abrir el DASHBOARD (lo que quieres visualizar)
```bash
cd ../analytics-streaming/dashboard
pip install -r requirements.txt

# Apuntar el dashboard a LocalStack:
export DYNAMODB_ENDPOINT=http://localhost:4566
export DYNAMODB_TABLA=bikenexus-estado-estaciones
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

streamlit run dashboard.py
```
Se abre en el navegador (http://localhost:8501). Verás las estaciones **vacías**
y **llenas** reales de Nueva York, ordenadas, con colores. 🚲

### 7. Ver los datos "a mano" (opcional)
```bash
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1

# Cuántos registros hay en DynamoDB:
aws --endpoint-url=http://localhost:4566 dynamodb scan \
  --table-name bikenexus-estado-estaciones --select COUNT

# Ver lo guardado en Bronze (S3):
aws --endpoint-url=http://localhost:4566 s3 ls s3://bikenexus-datalake/bronze/estaciones/
```

### 8. Apagar todo al terminar
```bash
cd ../../local
docker compose down
```

---

## Flujo para ver el dashboard actualizarse

### Opción A — Manual (una foto nueva cada vez)
```bash
python run_producer_local.py --ciclos 1   # trae datos frescos
python run_pipeline_local.py              # los procesa
# y refresca el dashboard en el navegador (botón "Refrescar ahora")
```

### Opción B — Automático en bucle (imita AWS) ⭐ recomendado
En vez de hacerlo a mano, deja un script corriendo que trae y procesa datos cada
60 segundos solo, igual que EventBridge + Spark Streaming en AWS:
```bash
python run_loop_local.py              # ciclo cada 60s
# o cada 30s:
python run_loop_local.py --intervalo 30
```
Déjalo corriendo en una ventana, abre el dashboard en otra, y pulsa "Refrescar
ahora" cuando quieras: siempre verás datos frescos. Para detener: Ctrl+C.

> En AWS esto es automático (EventBridge dispara cada 60s y Spark procesa en
> continuo). `run_loop_local.py` reproduce ese comportamiento en tu PC.

## ¿Qué NO se prueba en local?
- **EMR y Step Functions**: necesitan AWS real (la lógica de Spark sí se replica
  en `run_pipeline_local.py` para que veas el flujo).
- **El camino batch completo** (CSV mensual): se prueba mejor en AWS, aunque los
  jobs de Spark batch se pueden correr en local con Spark instalado.

## Problemas comunes
| Síntoma | Causa / solución |
|---------|------------------|
| `Could not connect to localhost:4566` | LocalStack no está arriba → `docker compose up -d` |
| El productor dice 0 estaciones | Sin internet, o el feed cambió → revisa conexión |
| El dashboard sale vacío | ¿Corriste el productor Y el pipeline antes? |
| `NoCredentialsError` | Faltan los `export AWS_ACCESS_KEY_ID=test ...` |
| `Stream ... not found` | El stream no se creó. En Windows usa `.\setup_local.ps1` (no `bash setup_local.sh`) |
| `$'\r': command not found` en bash | El `.sh` quedó con finales de línea de Windows → usa la versión `.ps1` |
| `aws: cannot execute` en bash (Windows) | Mezcla de bash + aws de Windows → usa PowerShell y `.\setup_local.ps1` |
