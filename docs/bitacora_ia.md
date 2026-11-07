# Bitácora de delegación de agentes de IA — BikeNexus

> Documento obligatorio (política de uso de IA del proyecto final). Registra qué
> hizo el agente de IA y qué decidió el equipo. El equipo entiende y puede
> defender todo el código entregado.

## Agente utilizado
- **Kiro** (agente de IA en entorno web), usado de forma interactiva y guiada por el equipo.

## Principio de trabajo
El equipo tomó todas las **decisiones de arquitectura y diseño** (ya definidas en
el E1: Kinesis, Spark, Delta Lake, DynamoDB, Step Functions). El agente se usó
para **generar el esqueleto y la implementación del código** a partir de esas
decisiones, y para **resolver problemas técnicos** durante el despliegue. Cada
pieza fue revisada y entendida por el equipo.

## Qué hizo el agente (por componente)

| Componente | Qué generó el agente | Qué decidió / validó el equipo |
|------------|----------------------|--------------------------------|
| Lambda productora | Código Python que consulta GBFS de Citi Bike y publica en Kinesis con reintentos | El equipo definió el contrato de datos, la partition key (estacion_id) y la garantía at-least-once (del E1) |
| Spark streaming (Bronze/Silver) | Jobs PySpark de ingesta cruda y limpieza/dedup | El equipo definió las reglas de calidad (RF-04) y la clave de dedup (RNF-03) |
| Clasificación (ventanas) | Lógica de ventana deslizante 15min/5min y clasificación | El equipo definió los umbrales y la lógica de tendencia (RF-06, del E1) |
| DynamoDB + dashboard | Escritura idempotente + dashboard Streamlit | El equipo definió el GSI (clasificación+tiempo) y la postura AP (RNF-02, del E1) |
| Spark batch (Gold) | Star schema + agregados (join, groupBy, window) | El equipo definió el modelo dimensional (RF-08, del E1) |
| Infraestructura Terraform | IaC de todos los recursos AWS | El equipo definió el stack (del E1) y resolvió las restricciones de Academy |
| Athena | DDL y consultas analíticas | El equipo definió las preguntas de negocio a responder |

## Problemas técnicos resueltos durante el despliegue (documentados)

Estos problemas reales se enfrentaron y resolvieron en AWS Academy. El equipo
entiende cada causa y solución (útil para la defensa):

1. **SCP de Academy bloquea object-lock de S3** → Terraform no podía gestionar
   buckets. Solución: crear los buckets con AWS CLI y referenciarlos por variable.
2. **Kinesis: IncreaseStreamRetentionPeriod bloqueado** → se quitó el valor
   explícito (24h es el default, que es lo que exige RNF-05).
3. **Runtime python3.11 no soportado por el proveedor AWS 4.67** → se usó python3.10.
4. **EMR: LabRole sin permisos EC2** → se usaron los roles estándar EMR_DefaultRole / EMR_EC2_DefaultRole.
5. **Conector Spark-Kinesis falla en EMR** → para la demo en vivo se usó un
   consumidor con boto3 (misma lógica); el job Spark nativo queda en el repo
   (emr_pipeline_aws.py). El Spark batch (viajes→Gold) sí corre en EMR.

## Qué NO delegó el equipo
- Las decisiones de arquitectura (están en el E1, justificadas con ADRs).
- La elección de tecnologías y sus garantías (CAP, entrega, modelo dimensional).
- La interpretación de los requisitos de negocio.

## Verificación de comprensión
Cada integrante puede explicar: el flujo de datos extremo a extremo, por qué
Kinesis y no Kafka, cómo se logra el efecto exactly-once en Silver, por qué
DynamoDB es AP, y cómo el star schema responde las preguntas de negocio.
