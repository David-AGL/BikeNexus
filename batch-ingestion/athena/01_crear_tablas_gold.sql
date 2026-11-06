-- =============================================================================
-- Registro de las tablas de Gold en Glue Data Catalog (RF-08)
-- =============================================================================
-- Estas sentencias se ejecutan en Athena. Crean una base de datos y registran
-- las tablas del star schema (Gold) apuntando a su ubicacion en S3.
--
-- Despues de esto, Athena puede consultar Gold con SQL normal, y las tablas
-- quedan visibles en el Glue Data Catalog.
--
-- NOTA: ajusta 's3://bikenexus-datalake/gold/...' a tu bucket real.
--       Las tablas Delta en Athena se leen como tablas externas Parquet; si
--       usas tablas Delta nativas, se recomienda generar el manifiesto o usar
--       un crawler de Glue (ver 03_crawler_glue.md).
-- =============================================================================

-- 1. Base de datos en el catalogo.
CREATE DATABASE IF NOT EXISTS bikenexus_gold
COMMENT 'Capa Gold de BikeNexus: star schema de viajes';

-- 2. Dimension de estaciones.
CREATE EXTERNAL TABLE IF NOT EXISTS bikenexus_gold.dim_estacion (
    estacion_id STRING,
    nombre      STRING,
    lat         DOUBLE,
    lon         DOUBLE,
    capacidad   INT,
    zona        STRING
)
STORED AS PARQUET
LOCATION 's3://bikenexus-datalake/gold/dim_estacion/';

-- 3. Dimension de tiempo.
CREATE EXTERNAL TABLE IF NOT EXISTS bikenexus_gold.dim_tiempo (
    tiempo_id   TIMESTAMP,
    anio        INT,
    mes         INT,
    dia         INT,
    hora        INT,
    dia_semana  INT,
    nombre_dia  STRING
)
STORED AS PARQUET
LOCATION 's3://bikenexus-datalake/gold/dim_tiempo/';

-- 4. Tabla de hechos de viajes (particionada por anio y mes).
CREATE EXTERNAL TABLE IF NOT EXISTS bikenexus_gold.fact_viajes (
    estacion_origen_id  STRING,
    estacion_destino_id STRING,
    tiempo_id           TIMESTAMP,
    inicio_ts           TIMESTAMP,
    fin_ts              TIMESTAMP,
    duracion_min        DOUBLE
)
PARTITIONED BY (anio INT, mes INT)
STORED AS PARQUET
LOCATION 's3://bikenexus-datalake/gold/fact_viajes/';

-- 5. Cargar las particiones existentes (necesario tras crear la tabla).
MSCK REPAIR TABLE bikenexus_gold.fact_viajes;
