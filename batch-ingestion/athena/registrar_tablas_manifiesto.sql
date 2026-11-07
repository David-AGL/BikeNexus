-- =============================================================================
-- Registrar las tablas de Gold en Athena apuntando al MANIFIESTO de Delta (RF-08)
-- =============================================================================
-- Prerequisito: ejecutar antes 'generar_manifiesto.py', que crea en cada tabla
-- la carpeta '_symlink_format_manifest/'. Aqui registramos las tablas usando
-- SymlinkTextInputFormat, que lee ese manifiesto y asi Athena ve SOLO los
-- .parquet vigentes de Delta (no versiones viejas, no datos duplicados).
--
-- Ejecutar en la consola de Athena (base de datos bikenexus_gold), una por una
-- o todo el bloque. El bucket real es bikenexus-datalake-athina2026.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS bikenexus_gold;

-- -----------------------------------------------------------------------------
-- 1. agg_demanda_estacion  (la tabla que responde la pregunta de negocio del E1)
--    Se borra la version anterior (registrada como Parquet plano, salia vacia).
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS bikenexus_gold.agg_demanda_estacion;

CREATE EXTERNAL TABLE bikenexus_gold.agg_demanda_estacion (
    estacion_id           STRING,
    nombre                STRING,
    zona                  STRING,
    nombre_dia            STRING,
    hora                  INT,
    num_viajes            BIGINT,
    duracion_promedio_min DOUBLE,
    duracion_max_min      DOUBLE,
    rank_en_zona          INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://bikenexus-datalake-athina2026/gold/agg_demanda_estacion/_symlink_format_manifest/';

-- -----------------------------------------------------------------------------
-- 2. dim_estacion
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS bikenexus_gold.dim_estacion;

CREATE EXTERNAL TABLE bikenexus_gold.dim_estacion (
    estacion_id STRING,
    nombre      STRING,
    lat         DOUBLE,
    lon         DOUBLE,
    capacidad   INT,
    zona        STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://bikenexus-datalake-athina2026/gold/dim_estacion/_symlink_format_manifest/';

-- -----------------------------------------------------------------------------
-- 3. dim_tiempo
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS bikenexus_gold.dim_tiempo;

CREATE EXTERNAL TABLE bikenexus_gold.dim_tiempo (
    tiempo_id   TIMESTAMP,
    anio        INT,
    mes         INT,
    dia         INT,
    hora        INT,
    dia_semana  INT,
    nombre_dia  STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://bikenexus-datalake-athina2026/gold/dim_tiempo/_symlink_format_manifest/';

-- -----------------------------------------------------------------------------
-- 4. fact_viajes
--    OJO: en Delta, las columnas de particion (anio, mes) van como columnas
--    normales dentro del manifiesto, NO como PARTITIONED BY. Por eso aqui se
--    declaran como columnas regulares y NO se usa MSCK REPAIR.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS bikenexus_gold.fact_viajes;

CREATE EXTERNAL TABLE bikenexus_gold.fact_viajes (
    estacion_origen_id  STRING,
    estacion_destino_id STRING,
    tiempo_id           TIMESTAMP,
    inicio_ts           TIMESTAMP,
    fin_ts              TIMESTAMP,
    duracion_min        DOUBLE,
    anio                INT,
    mes                 INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://bikenexus-datalake-athina2026/gold/fact_viajes/_symlink_format_manifest/';

-- -----------------------------------------------------------------------------
-- Verificacion rapida (debe devolver filas, no vacio):
--   SELECT zona, nombre, hora, num_viajes, duracion_promedio_min, rank_en_zona
--   FROM bikenexus_gold.agg_demanda_estacion
--   WHERE rank_en_zona <= 3
--   ORDER BY zona, rank_en_zona;
-- =============================================================================
