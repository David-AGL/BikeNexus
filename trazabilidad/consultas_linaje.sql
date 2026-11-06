-- =============================================================================
-- Consultas de auditoría de linaje (RNF-06)
-- =============================================================================
-- Demuestran como rastrear un dato desde Gold hasta su fuente original.
-- Se ejecutan en Spark SQL (o Athena, segun la capa).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- A. Ver el HISTORIAL DE VERSIONES de una tabla (time travel, RNF-06).
--    Muestra cada version, cuando se escribio y que operacion la genero.
-- -----------------------------------------------------------------------------
DESCRIBE HISTORY delta.`s3://bikenexus-datalake-eafit/silver/viajes`;


-- -----------------------------------------------------------------------------
-- B. LINAJE BATCH: de un viaje en Silver a su archivo CSV de origen.
--    La columna meta_archivo_origen indica el CSV exacto del que vino.
-- -----------------------------------------------------------------------------
SELECT
    estacion_origen,
    estacion_destino,
    inicio_ts,
    duracion_seg,
    meta_archivo_origen,     -- <- archivo fuente exacto
    meta_bronze_ingest_ts    -- <- cuando entro a Bronze
FROM delta.`s3://bikenexus-datalake-eafit/silver/viajes`
LIMIT 10;


-- -----------------------------------------------------------------------------
-- C. LINAJE STREAMING: de un registro de estacion a su mensaje en Kinesis.
--    shard_id + sequence_number identifican el mensaje exacto de Kinesis.
-- -----------------------------------------------------------------------------
SELECT
    estacion_id,
    timestamp,
    bikes_disponibles,
    meta_stream_name,        -- <- stream de Kinesis
    meta_shard_id,           -- <- shard
    meta_sequence_number,    -- <- posicion exacta en el shard
    meta_bronze_ingest_ts    -- <- cuando entro a Bronze
FROM delta.`s3://bikenexus-datalake-eafit/bronze/estaciones`
WHERE estacion_id = '72'
ORDER BY timestamp DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- D. VERIFICAR INMUTABILIDAD: comparar un registro entre Bronze y Silver.
--    Confirma que el dato crudo de Bronze sigue intacto tras la limpieza.
-- -----------------------------------------------------------------------------
SELECT 'bronze' AS capa, estacion_id, timestamp, bikes_disponibles
FROM delta.`s3://bikenexus-datalake-eafit/bronze/estaciones`
WHERE estacion_id = '72' AND timestamp = '2026-11-05T10:30:00Z'
UNION ALL
SELECT 'silver' AS capa, estacion_id, timestamp, bikes_disponibles
FROM delta.`s3://bikenexus-datalake-eafit/silver/estaciones`
WHERE estacion_id = '72' AND timestamp = '2026-11-05T10:30:00Z';
