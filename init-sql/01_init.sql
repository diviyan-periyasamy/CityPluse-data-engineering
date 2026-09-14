-- Runs automatically on first Postgres container startup
-- (docker-entrypoint-initdb.d). This same Postgres instance already holds
-- the 'airflow' database (Airflow's own metadata, created via
-- POSTGRES_DB in docker-compose.yml). Here we create a SEPARATE database,
-- 'weatherdb', to hold our own staging and warehouse schemas, so the
-- project's data is cleanly isolated from Airflow's internal tables.

CREATE DATABASE weatherdb;

\c weatherdb

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- ============================================================
-- STAGING: raw data, exactly as received from Kafka. Never cleaned here.
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.weather_raw (
    id               BIGSERIAL PRIMARY KEY,
    city_name        TEXT NOT NULL,
    raw_payload      JSONB NOT NULL,
    kafka_partition  INT NOT NULL,
    kafka_offset     BIGINT NOT NULL,
    ingested_at      TIMESTAMP NOT NULL DEFAULT now(),
    -- Kafka (partition, offset) is unique per message. Enforcing that here
    -- is what makes the consumer idempotent: re-running it never inserts
    -- the same message twice.
    UNIQUE (kafka_partition, kafka_offset)
);

-- ============================================================
-- WAREHOUSE: clean, analytics-ready star schema.
-- ============================================================
CREATE TABLE IF NOT EXISTS warehouse.dim_city (
    city_id    SERIAL PRIMARY KEY,
    city_name  TEXT UNIQUE NOT NULL,
    latitude   DOUBLE PRECISION NOT NULL,
    longitude  DOUBLE PRECISION NOT NULL
);

INSERT INTO warehouse.dim_city (city_name, latitude, longitude) VALUES
    ('Colombo',  6.9271,   79.8612),
    ('London',   51.5072, -0.1276),
    ('New York', 40.7128, -74.0060),
    ('Tokyo',    35.6895,  139.6917),
    ('Sydney',  -33.8688,  151.2093)
ON CONFLICT (city_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS warehouse.fact_weather_hourly (
    city_id            INT NOT NULL REFERENCES warehouse.dim_city(city_id),
    reading_time       TIMESTAMP NOT NULL,
    temperature_c      DOUBLE PRECISION,
    humidity_pct       DOUBLE PRECISION,
    precipitation_mm   DOUBLE PRECISION,
    wind_speed_kmh     DOUBLE PRECISION,
    weather_category   TEXT,
    PRIMARY KEY (city_id, reading_time)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_weather_daily (
    city_id                     INT NOT NULL REFERENCES warehouse.dim_city(city_id),
    reading_date                DATE NOT NULL,
    min_temperature_c           DOUBLE PRECISION,
    max_temperature_c           DOUBLE PRECISION,
    avg_temperature_c           DOUBLE PRECISION,
    total_precipitation_mm      DOUBLE PRECISION,
    avg_wind_speed_kmh          DOUBLE PRECISION,
    dominant_weather_category   TEXT,
    PRIMARY KEY (city_id, reading_date)
);
