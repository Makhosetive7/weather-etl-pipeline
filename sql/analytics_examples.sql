-- Example analytics queries for the weather warehouse
-- Run: docker compose exec postgres psql -U postgres -d weather_analytics -f sql/analytics_examples.sql
--
-- HTTP equivalents (FastAPI read API):
--   Latest per city     -> GET /api/v1/weather/latest
--   Avg temp (7 days)   -> GET /api/v1/analytics/temperature-summary?days=7
--   Hottest / coldest   -> GET /api/v1/analytics/extremes
--   Recent ETL runs     -> GET /api/v1/analytics/etl-runs

-- Latest weather per city (built-in view)
SELECT * FROM weather.latest_weather ORDER BY city_name;

-- Average temperature by city (last 7 days)
SELECT
    c.city_name,
    ROUND(AVG(wm.temperature_celsius)::numeric, 2) AS avg_temp_c
FROM weather.weather_measurements wm
JOIN weather.cities c ON wm.city_id = c.city_id
WHERE wm.measurement_timestamp >= NOW() - INTERVAL '7 days'
GROUP BY c.city_name
ORDER BY avg_temp_c DESC;

-- Hottest city in the latest snapshot
SELECT city_name, temperature_celsius, measurement_timestamp
FROM weather.latest_weather
ORDER BY temperature_celsius DESC
LIMIT 1;

-- Coldest city in the latest snapshot
SELECT city_name, temperature_celsius, measurement_timestamp
FROM weather.latest_weather
ORDER BY temperature_celsius ASC
LIMIT 1;

-- Row counts by table
SELECT 'cities' AS table_name, COUNT(*) AS row_count FROM weather.cities
UNION ALL
SELECT 'conditions', COUNT(*) FROM weather.weather_conditions
UNION ALL
SELECT 'measurements', COUNT(*) FROM weather.weather_measurements;

-- Recent ETL pipeline runs
SELECT
    run_id,
    status,
    records_extracted,
    records_transformed,
    records_loaded,
    records_failed,
    started_at,
    finished_at,
    EXTRACT(EPOCH FROM (finished_at - started_at)) AS duration_seconds
FROM weather.etl_runs
ORDER BY started_at DESC
LIMIT 10;
