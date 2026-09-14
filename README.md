# CityPulse: A Weather Analytics Pipeline

An end-to-end data engineering pipeline that collects live weather data for
five cities, streams it through Kafka, lands it in a PostgreSQL staging
table, transforms it with PySpark into an analytics-ready warehouse, and
answers analytical questions with SQL — all orchestrated by Airflow and run
locally with Docker Compose.

## Architecture

```
                     hourly (@hourly)
   Open-Meteo API  ─────────────────▶  Airflow DAG: ingest_weather
   (5 fixed cities)                      ├─ task 1: produce_weather_readings
                                          │      (publishes 1 JSON msg/city)
                                          ▼
                                    Kafka topic: weather_raw
                                          │
                                          ▼
                                      task 2: load_to_staging
                                          │  (idempotent insert, dedup on
                                          │   kafka partition+offset)
                                          ▼
                              PostgreSQL  staging.weather_raw
                                          │
                                          │  daily (@daily)
                                          ▼
                              Airflow DAG: transform_weather
                                          │  runs spark/transform_job.py
                                          │  (local-mode PySpark, JDBC)
                                          │  - parse JSON, dedupe, clean nulls
                                          │  - map weather_code -> category
                                          │  - aggregate to daily stats
                                          ▼
                    PostgreSQL warehouse.dim_city
                    PostgreSQL warehouse.fact_weather_hourly
                    PostgreSQL warehouse.fact_weather_daily
                                          │
                                          ▼
                              sql/analytics.sql  (5 analytical queries)
```

One Postgres container serves two roles: the `airflow` database holds
Airflow's own metadata, and a separate `weatherdb` database (created by
`init-sql/01_init.sql` on first startup) holds the `staging` and
`warehouse` schemas.

A third DAG, **`backfill_weather`**, is a manual one-off that seeds several
days of *real* historical data (via Open-Meteo's `past_days` parameter)
through the same Kafka → staging path, so the warehouse has enough history
to run meaningful analytics without waiting days for the hourly DAG to
accumulate it live. See the report for why this is a reasonable approach.

## Components

| Component | What it does |
|---|---|
| `docker-compose.yml` | Spins up Postgres, a single-broker Kafka (KRaft mode, no Zookeeper needed), and Airflow (webserver + scheduler), all on one Docker network. |
| `airflow/Dockerfile` | Extends the base Airflow image with Java (for PySpark), the `kafka-python` client, and the Postgres JDBC driver. |
| `init-sql/01_init.sql` | Runs once on first Postgres startup. Creates `weatherdb`, and the `staging`/`warehouse` schemas and tables. |
| `dags/ingest_weather.py` | Hourly DAG. Fetches all 5 cities, publishes to Kafka, then loads new messages into `staging.weather_raw`. |
| `dags/backfill_weather.py` | Manual-trigger DAG. Backfills N days of historical data through the same pipeline. Run this once, early. |
| `dags/transform_weather.py` | Daily DAG. Triggers the PySpark job. |
| `dags/common/` | Shared helper modules: `weather_api.py` (API calls), `kafka_utils.py` (producer/consumer), `staging_loader.py` (shared insert logic). |
| `spark/transform_job.py` | PySpark job: reads staging via JDBC, cleans/dedupes/aggregates, writes the three warehouse tables. |
| `sql/analytics.sql` | The five analytical queries required by the coursework. |

## How to run this from scratch

**Prerequisites:** Docker Desktop (or Docker Engine + Compose) installed and running.

1. Open a terminal in this project folder (the one containing `docker-compose.yml`).

2. Build and start everything:
   ```
   docker compose up -d --build
   ```
   First run takes a few minutes (downloading images, building the custom
   Airflow image, installing PySpark). Watch progress with:
   ```
   docker compose logs -f
   ```

3. Check everything is healthy:
   ```
   docker compose ps
   ```
   You want `citypulse-postgres`, `citypulse-kafka`, `citypulse-airflow-webserver`
   and `citypulse-airflow-scheduler` all showing `healthy` / `Up`.
   `citypulse-airflow-init` should show `Exited (0)` — that's expected, it's a
   one-off setup container.

4. Open the Airflow UI: **http://localhost:8080** — login `admin` / `admin`.

5. **Backfill historical data (run once):**
   In the Airflow UI, go to DAGs → `backfill_weather` → click the ▶ (trigger)
   button → "Trigger DAG w/ config" → enter:
   ```json
   {"days_back": 10}
   ```
   Wait for both tasks to go green (a minute or two — it's fetching and
   inserting ~1,200 rows per city).

6. **Unpause the two scheduled DAGs**: toggle `ingest_weather` and
   `transform_weather` to "on" (unpaused) in the DAGs list. `ingest_weather`
   will pick up on the next hour boundary; you can also trigger it manually
   right away to add the freshest reading.

7. **Run the transform job**: trigger `transform_weather` manually (no
   config needed) once there's data in staging. Watch the task log — it
   prints row counts as it writes each warehouse table.

8. **Check the results** in Postgres:
   ```
   docker exec -it citypulse-postgres psql -U airflow -d weatherdb
   ```
   Then, inside psql:
   ```sql
   SELECT count(*) FROM staging.weather_raw;
   SELECT * FROM warehouse.dim_city;
   SELECT * FROM warehouse.fact_weather_daily ORDER BY reading_date, city_id;
   ```

9. **Run the analytics queries**:
   ```
   docker exec -it citypulse-postgres psql -U airflow -d weatherdb -f /dev/stdin < sql/analytics.sql
   ```
   (or open `sql/analytics.sql` and paste the queries into psql / a DB
   client like DBeaver / pgAdmin, one at a time.)

10. **See messages flowing through Kafka** (nice for the demo):
    ```
    docker exec -it citypulse-kafka /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 --topic weather_raw --from-beginning --max-messages 5
    ```

## Shutting down

```
docker compose down          # stop everything, keep data
docker compose down -v       # stop everything AND wipe the database volumes
```

## Design notes

- **Overwrite vs. upsert**: the Spark job does a full overwrite of the
  warehouse fact tables on every run (see comment at the top of
  `spark/transform_job.py` for the reasoning).
- **Idempotency**: `staging.weather_raw` has a unique constraint on
  `(kafka_partition, kafka_offset)`, and the consumer only commits a Kafka
  offset after the matching DB insert succeeds — so re-running the load
  task never creates duplicate rows.
- Full discussion of design decisions and problems hit is in the report.
