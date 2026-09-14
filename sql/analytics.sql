-- CityPulse Analytics Queries
-- Run against the 'weatherdb' database.
-- e.g.  docker exec -it citypulse-postgres psql -U airflow -d weatherdb -f /path/to/analytics.sql

-- 1. Hottest and coldest city each day
SELECT
    reading_date,
    (SELECT c.city_name FROM warehouse.fact_weather_daily d2
        JOIN warehouse.dim_city c ON c.city_id = d2.city_id
        WHERE d2.reading_date = d.reading_date
        ORDER BY d2.max_temperature_c DESC LIMIT 1) AS hottest_city,
    (SELECT c.city_name FROM warehouse.fact_weather_daily d2
        JOIN warehouse.dim_city c ON c.city_id = d2.city_id
        WHERE d2.reading_date = d.reading_date
        ORDER BY d2.min_temperature_c ASC LIMIT 1) AS coldest_city
FROM warehouse.fact_weather_daily d
GROUP BY reading_date
ORDER BY reading_date;

-- 2. Average daily temperature range (max - min) per city
SELECT
    c.city_name,
    ROUND(AVG(d.max_temperature_c - d.min_temperature_c)::numeric, 2) AS avg_daily_range_c
FROM warehouse.fact_weather_daily d
JOIN warehouse.dim_city c ON c.city_id = d.city_id
GROUP BY c.city_name
ORDER BY avg_daily_range_c DESC;

-- 3. City with the most total precipitation over the collection period
SELECT
    c.city_name,
    ROUND(SUM(d.total_precipitation_mm)::numeric, 2) AS total_precipitation_mm
FROM warehouse.fact_weather_daily d
JOIN warehouse.dim_city c ON c.city_id = d.city_id
GROUP BY c.city_name
ORDER BY total_precipitation_mm DESC;

-- 4. Windiest city on average
SELECT
    c.city_name,
    ROUND(AVG(h.wind_speed_kmh)::numeric, 2) AS avg_wind_speed_kmh
FROM warehouse.fact_weather_hourly h
JOIN warehouse.dim_city c ON c.city_id = h.city_id
GROUP BY c.city_name
ORDER BY avg_wind_speed_kmh DESC;

-- 5. Hours recorded in each weather category, per city
SELECT
    c.city_name,
    h.weather_category,
    COUNT(*) AS hours_recorded
FROM warehouse.fact_weather_hourly h
JOIN warehouse.dim_city c ON c.city_id = h.city_id
GROUP BY c.city_name, h.weather_category
ORDER BY c.city_name, hours_recorded DESC;
