"""
ingest_weather DAG
-------------------
Runs hourly. Two tasks:

  1. produce_weather_readings - fetch all 5 cities from Open-Meteo and
     publish each reading as a JSON message to the Kafka topic 'weather_raw'.

  2. load_to_staging - consume any new messages from 'weather_raw' and
     insert them, unchanged, into staging.weather_raw. Uses the Kafka
     (partition, offset) as a uniqueness key so re-running never creates
     duplicate rows, and only commits Kafka offsets after the DB write
     succeeds - this is what makes the load idempotent.
"""
import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/dags")
from common.kafka_utils import get_producer, publish_reading
from common.staging_loader import load_to_staging
from common.weather_api import CITIES, get_weather

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "kafka:9092"
PG_CONN = dict(host="postgres", port=5432, dbname="weatherdb", user="airflow", password="airflow")

default_args = {
    "owner": "dulakshan",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def produce_weather_readings(**context):
    producer = get_producer(KAFKA_BOOTSTRAP)
    failures = []
    try:
        for city in CITIES:
            try:
                reading = get_weather(city)
                publish_reading(producer, reading)
            except Exception:
                logger.exception("Failed to fetch/publish %s", city)
                failures.append(city)
        producer.flush()
    finally:
        producer.close()

    if failures:
        # Fail the task so Airflow retries it, per the DAG's retry policy.
        raise RuntimeError(f"Failed to publish readings for: {failures}")


def load_to_staging_task(**context):
    load_to_staging(KAFKA_BOOTSTRAP, PG_CONN)


with DAG(
    dag_id="ingest_weather",
    description="Hourly: fetch weather for 5 cities -> Kafka -> Postgres staging",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["citypulse", "ingestion"],
) as dag:

    produce_task = PythonOperator(
        task_id="produce_weather_readings",
        python_callable=produce_weather_readings,
    )

    load_task = PythonOperator(
        task_id="load_to_staging",
        python_callable=load_to_staging_task,
    )

    produce_task >> load_task
