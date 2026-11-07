-- =============================================================================
-- Consultas analiticas de ejemplo (RF-08)
-- =============================================================================
-- Estas son las consultas que los analistas ejecutan en Athena sobre Gold.
-- RF-08 pide: numero de viajes y duracion promedio por estacion, hora y dia,
-- con respuesta < 5 segundos. El star schema y las particiones lo permiten.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Numero de viajes y duracion promedio POR ESTACION DE ORIGEN.
--    Responde: "que estaciones generan mas viajes?"
-- -----------------------------------------------------------------------------
SELECT
    e.estacion_id,
    e.nombre,
    e.zona,
    COUNT(*)                     AS num_viajes,
    ROUND(AVG(f.duracion_min), 2) AS duracion_promedio_min
FROM bikenexus_gold.fact_viajes f
JOIN bikenexus_gold.dim_estacion e
  ON f.estacion_origen_id = e.estacion_id
GROUP BY e.estacion_id, e.nombre, e.zona
ORDER BY num_viajes DESC
LIMIT 20;

-- -----------------------------------------------------------------------------
-- 2. Numero de viajes y duracion promedio POR HORA DEL DIA.
--    Responde: "a que horas hay mas demanda?"
-- -----------------------------------------------------------------------------
SELECT
    t.hora,
    COUNT(*)                     AS num_viajes,
    ROUND(AVG(f.duracion_min), 2) AS duracion_promedio_min
FROM bikenexus_gold.fact_viajes f
JOIN bikenexus_gold.dim_tiempo t
  ON f.tiempo_id = t.tiempo_id
GROUP BY t.hora
ORDER BY t.hora;

-- -----------------------------------------------------------------------------
-- 3. Numero de viajes POR DIA DE LA SEMANA.
--    Responde: "la demanda cambia entre semana y fin de semana?"
-- -----------------------------------------------------------------------------
SELECT
    t.nombre_dia,
    t.dia_semana,
    COUNT(*)                     AS num_viajes,
    ROUND(AVG(f.duracion_min), 2) AS duracion_promedio_min
FROM bikenexus_gold.fact_viajes f
JOIN bikenexus_gold.dim_tiempo t
  ON f.tiempo_id = t.tiempo_id
GROUP BY t.nombre_dia, t.dia_semana
ORDER BY t.dia_semana;

-- -----------------------------------------------------------------------------
-- 4. Demanda POR ZONA Y FRANJA HORARIA (combina las dos dimensiones).
--    Responde la pregunta de gerencia: "donde y cuando concentrar recursos?"
-- -----------------------------------------------------------------------------
SELECT
    e.zona,
    t.hora,
    COUNT(*) AS num_viajes
FROM bikenexus_gold.fact_viajes f
JOIN bikenexus_gold.dim_estacion e ON f.estacion_origen_id = e.estacion_id
JOIN bikenexus_gold.dim_tiempo   t ON f.tiempo_id = t.tiempo_id
GROUP BY e.zona, t.hora
ORDER BY e.zona, t.hora;


-- -----------------------------------------------------------------------------
-- 5. Usando la tabla agregada de Gold (ya calculada por Spark con join + groupBy
--    + windowing): top 3 estaciones mas demandadas DE CADA ZONA.
--    Responde la pregunta de negocio del E1: "¿en que zonas se concentra la
--    demanda y cuales son las estaciones clave?" (RF-08).
-- -----------------------------------------------------------------------------
SELECT zona, nombre, hora, num_viajes, duracion_promedio_min, rank_en_zona
FROM bikenexus_gold.agg_demanda_estacion
WHERE rank_en_zona <= 3
ORDER BY zona, rank_en_zona;
