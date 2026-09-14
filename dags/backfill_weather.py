"""
backfill_weather DAG (manual trigger only)
-------------------------------------------
One-off DAG used to seed the warehouse with several days of history
immediately, rather than waiting for the hourly ingest_weather DAG to
accumulate that much data live. It fetches REAL historical hourly readings
for each city from Open-Meteo (past_days parameter), and publishes them to
Kafka exactly the way the live DAG does - so it exercises the same
Kafka -> Postgres path, just with a bulk historical load instead of one
hour at a time.

Trigger manually from the Airflow UI:
    DAGs -> backfill_weather -> Trigger DAG w/ config -> {"days_back": 10}

Only needs to be run once. After that, ingest_weather (hourly) takes over
and adds new live data on top of the backfilled history.
"""
import logging
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/dags")
from common.kafka_utils import get_producer, publish_reading
from common.staging_loader import load_to_staging
from common.weather_api import CITIES, get_weather_history

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "kafka:9092"
PG_CONN = dict(host="postgres", port=5432, dbname="weatherdb", user="airflow", password="airflow")

default_args = {"owner": "dulakshan", "retries": 1}


def produce_historical_readings(**context):
    dag_run = context.get("dag_run")
    days_back = (dag_run.conf or {}).get("days_back", 10) if dag_run else 10

    producer = get_producer(KAFKA_BOOTSTRAP)
    total = 0
    try:
        for city in CITIES:
            readings = get_weather_history(city, days_back=days_back)
            for r in readings:
                publish_reading(producer, r)
                total += 1
            logger.info("Published %s historical readings for %s", len(readings), city)
        producer.flush()
    finally:
        producer.close()
    logger.info("Published %s historical readings to Kafka in total", total)


def drain_to_staging(**context):
    # Larger consumer_timeout is not needed here - load_to_staging already
    # waits 10s of silence before deciding it has drained the topic.
    load_to_staging(KAFKA_BOOTSTRAP, PG_CONN, group_id="staging-loader")


with DAG(
    dag_id="backfill_weather",
    description="Manual one-off: backfill N days of historical weather via Open-Meteo past_days",
    default_args=default_args,
    schedule_interval=None,  # manual trigger only - never runs on a schedule
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["citypulse", "backfill"],
) as dag:

    produce_task = PythonOperator(
        task_id="produce_historical_readings",
        python_callable=produce_historical_readings,
    )

    load_task = PythonOperator(
        task_id="drain_to_staging",
        python_callable=drain_to_staging,
    )

    produce_task >> load_task
