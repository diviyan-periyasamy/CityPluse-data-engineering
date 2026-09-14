#  CityPulse: A Weather Analytics Pipeline

An end-to-end **data engineering pipeline** that collects live weather data for five cities, streams it through **Apache Kafka**, lands it in a **PostgreSQL staging database**, transforms it with **PySpark** into an analytics-ready warehouse, and answers analytical questions with **SQL**.

The entire pipeline is orchestrated by **Apache Airflow** and runs locally using **Docker Compose**.

---

##  Architecture

```text
                         hourly (@hourly)
      Open-Meteo API ──────────────────────▶ Airflow DAG: ingest_weather
      (5 fixed cities)                         │
                                               ├── Task 1:
                                               │   produce_weather_readings
                                               │   (1 JSON message/city)
                                               │
                                               ▼
                                      Kafka Topic: weather_raw
                                               │
                                               ▼
                                               ├── Task 2:
                                               │   load_to_staging
                                               │   (idempotent insert)
                                               │
                                               ▼
                              PostgreSQL staging.weather_raw
                                               │
                                               │
                                               │ daily (@daily)
                                               ▼
                              Airflow DAG: transform_weather
                                               │
                                               ▼
                                  spark/transform_job.py
                                  (Local-mode PySpark + JDBC)
                                               │
                         ┌─────────────────────┼─────────────────────┐
                         │                     │                     │
                         ▼                     ▼                     ▼
                    Parse JSON            Clean Data          Deduplicate
                         │                     │                     │
                         └─────────────────────┼─────────────────────┘
                                               │
                                               ▼
                                  Weather Code Mapping
                                               │
                                               ▼
                                    Daily Aggregations
                                               │
                                               ▼
                        ┌─────────────────────────────────────────────┐
                        │              PostgreSQL Warehouse           │
                        │                                             │
                        │  warehouse.dim_city                         │
                        │  warehouse.fact_weather_hourly              │
                        │  warehouse.fact_weather_daily               │
                        └──────────────────────┬──────────────────────┘
                                               │
                                               ▼
                                    sql/analytics.sql
                                      (5 SQL queries)
```

### Database Architecture

One PostgreSQL container serves two database roles:

* `airflow` — stores Airflow's internal metadata
* `weatherdb` — stores the project's weather data

The `weatherdb` database contains:

```text
weatherdb
│
├── staging
│   └── weather_raw
│
└── warehouse
    ├── dim_city
    ├── fact_weather_hourly
    └── fact_weather_daily
```

The database and schemas are initialized automatically by:

```text
init-sql/01_init.sql
```

### Historical Data Backfill

A third DAG, **`backfill_weather`**, is included to seed several days of real historical weather data using Open-Meteo's `past_days` parameter.

This allows the warehouse to contain sufficient historical data for meaningful analytics without waiting several days for the scheduled hourly pipeline to accumulate data.

The backfill follows the same:

```text
Open-Meteo
    ↓
Kafka
    ↓
PostgreSQL Staging
    ↓
PySpark
    ↓
Data Warehouse
```

workflow as the live pipeline.

---

##  Components

| Component                   | What it does                                                                                                              |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `docker-compose.yml`        | Spins up PostgreSQL, single-broker Kafka in KRaft mode, and Airflow webserver + scheduler on one Docker network           |
| `airflow/Dockerfile`        | Extends the Airflow image with Java, PySpark dependencies, `kafka-python`, and the PostgreSQL JDBC driver                 |
| `init-sql/01_init.sql`      | Initializes `weatherdb`, staging schemas, warehouse schemas, and tables                                                   |
| `dags/ingest_weather.py`    | Hourly DAG that fetches weather data for all five cities, publishes messages to Kafka, and loads new records into staging |
| `dags/backfill_weather.py`  | Manual DAG for loading historical weather data through the same Kafka → staging pipeline                                  |
| `dags/transform_weather.py` | Daily DAG that triggers the PySpark transformation job                                                                    |
| `dags/common/`              | Shared utilities for API calls, Kafka producers/consumers, and staging database operations                                |
| `spark/transform_job.py`    | PySpark transformation job that cleans, deduplicates, aggregates, and loads warehouse tables                              |
| `sql/analytics.sql`         | Contains the five analytical SQL queries required by the project                                                          |

---

## Tech Stack

| Category         | Technology      |
| ---------------- | --------------- |
| Language         | Python          |
| Data Source      | Open-Meteo API  |
| Streaming        | Apache Kafka    |
| Message Format   | JSON            |
| Orchestration    | Apache Airflow  |
| Processing       | PySpark         |
| Database         | PostgreSQL      |
| Query Language   | SQL             |
| Containerization | Docker          |
| Environment      | Docker Compose  |
| Connectivity     | PostgreSQL JDBC |
| Version Control  | Git / GitHub    |

---

#  How to Run From Scratch

## Prerequisites

Make sure you have:

* Docker Desktop or Docker Engine
* Docker Compose
* Git

Docker must be installed and running before starting the project.

---

## 1. Clone the Repository

```bash
git clone https://github.com/DhulakshanKannan/citypulse-data-engineering.git
cd citypulse-data-engineering
```

---

## 2. Build and Start the Pipeline

Run:

```bash
docker compose up -d --build
```

The first run may take several minutes because Docker needs to:

* Download required images
* Build the custom Airflow image
* Install Java dependencies
* Install PySpark
* Configure the Kafka broker
* Initialize PostgreSQL

To monitor the startup process:

```bash
docker compose logs -f
```

---

## 3. Check Container Health

Run:

```bash
docker compose ps
```

You should see the following services running:

```text
citypulse-postgres
citypulse-kafka
citypulse-airflow-webserver
citypulse-airflow-scheduler
```

The one-time initialization container:

```text
citypulse-airflow-init
```

should show:

```text
Exited (0)
```

This is expected.

---

## 4. Open the Airflow UI

Open:

```text
http://localhost:8080
```

Default credentials:

```text
Username: admin
Password: admin
```

---

#  5. Backfill Historical Weather Data

Run the backfill once before starting the regular scheduled pipeline.

In the Airflow UI:

```text
DAGs
 ↓
backfill_weather
 ↓
Trigger DAG
 ↓
Trigger DAG w/ config
```

Use:

```json
{
  "days_back": 10
}
```

The DAG will fetch historical weather data and send it through:

```text
Open-Meteo
     ↓
Kafka
     ↓
staging.weather_raw
```

Wait until both tasks complete successfully.

---

#  6. Enable the Scheduled DAGs

In the Airflow DAG list, enable:

```text
ingest_weather
transform_weather
```

### `ingest_weather`

Runs hourly and:

1. Fetches weather data for five cities
2. Produces JSON messages
3. Publishes them to Kafka
4. Consumes the messages
5. Loads them into PostgreSQL staging

### `transform_weather`

Runs daily and:

1. Reads staging data
2. Cleans and validates the data
3. Removes duplicates
4. Maps weather codes to categories
5. Calculates daily statistics
6. Updates the warehouse tables

You can also manually trigger the ingestion DAG for an immediate reading.

---

#  7. Run the Transformation

Once weather data exists in the staging table, trigger:

```text
transform_weather
```

from the Airflow UI.

The PySpark job will process the staging data and populate:

```text
warehouse.dim_city
warehouse.fact_weather_hourly
warehouse.fact_weather_daily
```

The task logs display row counts as the warehouse tables are written.

---

#  8. Check PostgreSQL Results

Connect to the weather database:

```bash
docker exec -it citypulse-postgres psql -U airflow -d weatherdb
```

Then run:

```sql
SELECT count(*)
FROM staging.weather_raw;
```

Check the city dimension:

```sql
SELECT *
FROM warehouse.dim_city;
```

Check the daily weather facts:

```sql
SELECT *
FROM warehouse.fact_weather_daily
ORDER BY reading_date, city_id;
```

---

#  9. Run the Analytics Queries

The project includes five analytical SQL queries in:

```text
sql/analytics.sql
```

Run them directly with:

```bash
docker exec -it citypulse-postgres \
psql -U airflow -d weatherdb -f /dev/stdin < sql/analytics.sql
```

Alternatively, open:

```text
sql/analytics.sql
```

and execute the queries individually using:

* `psql`
* DBeaver
* pgAdmin
* Another PostgreSQL client

---

#  10. Monitor Kafka Messages

To demonstrate the streaming pipeline, consume messages directly from the Kafka topic:

```bash
docker exec -it citypulse-kafka \
/opt/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic weather_raw \
--from-beginning \
--max-messages 5
```

You should see JSON weather messages flowing through:

```text
Open-Meteo API
      ↓
Kafka: weather_raw
```

---

#  Shutting Down

Stop the services while preserving database volumes:

```bash
docker compose down
```

Stop the services and remove the database volumes:

```bash
docker compose down -v
```

>  `docker compose down -v` permanently removes the PostgreSQL volumes and therefore deletes the stored project data.

---

#  Design Notes

## 1. Overwrite vs Upsert

The PySpark transformation job performs a **full overwrite of the warehouse fact tables** on each run.

This design choice is documented in:

```text
spark/transform_job.py
```

The approach keeps the transformation process straightforward and ensures the warehouse is rebuilt from the current staging dataset.

---

## 2. Idempotency

The staging pipeline is designed to prevent duplicate Kafka records.

The table:

```text
staging.weather_raw
```

uses a unique constraint based on:

```text
(kafka_partition, kafka_offset)
```

The Kafka consumer commits an offset only after the corresponding database insert succeeds.

Therefore, if the load task is re-run:

```text
Kafka Message
     ↓
Database Insert
     ↓
Success
     ↓
Commit Kafka Offset
```

If the database insert fails, the Kafka offset is not committed, allowing the message to be processed again safely.

---

## 3. Data Flow

The complete pipeline can be summarized as:

```text
        Open-Meteo API
               │
               ▼
       Apache Airflow
               │
               ▼
       Apache Kafka
       weather_raw
               │
               ▼
        PostgreSQL
          Staging
               │
               ▼
           PySpark
               │
       ┌───────┴────────┐
       │                │
       ▼                ▼
   Cleaning         Aggregation
       │                │
       └───────┬────────┘
               ▼
      PostgreSQL Warehouse
               │
       ┌───────┼────────┐
       ▼       ▼        ▼
    dim_city  hourly   daily
               │
               ▼
             SQL
          Analytics
```

---

#  Project Structure

```text
citypulse-data-engineering/
│
├── airflow/
│   └── Dockerfile
│
├── dags/
│   ├── ingest_weather.py
│   ├── backfill_weather.py
│   ├── transform_weather.py
│   │
│   └── common/
│       ├── weather_api.py
│       ├── kafka_utils.py
│       └── staging_loader.py
│
├── spark/
│   └── transform_job.py
│
├── sql/
│   └── analytics.sql
│
├── init-sql/
│   └── 01_init.sql
│
├── docker-compose.yml
└── README.md
```

---

# 🎯 Key Data Engineering Concepts Demonstrated

This project demonstrates practical experience with:

* **ETL / ELT pipelines**
* **Batch and streaming data processing**
* **Apache Kafka**
* **Apache Airflow**
* **PySpark**
* **PostgreSQL**
* **Data warehouse design**
* **Dimensional modeling**
* **Data cleaning and transformation**
* **Data aggregation**
* **Idempotent data ingestion**
* **Kafka offset management**
* **Docker containerization**
* **SQL analytics**
* **Pipeline orchestration**

---

#  Author

**Diviyan Periyasmay**

BSc (Hons) Data Science, Coventry University | NIBM

Developed as part of an **HND Data Engineering / Data Science project**.

---

⭐ If you find this project useful, consider giving the repository a star!
