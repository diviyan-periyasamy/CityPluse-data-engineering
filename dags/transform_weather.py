"""
transform_weather DAG
----------------------
Runs daily. Single task that launches the PySpark job (spark/transform_job.py)
in local mode. The job reads staging.weather_raw via JDBC, cleans it, and
rebuilds the warehouse tables: dim_city, fact_weather_hourly, fact_weather_daily.
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "dulakshan",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def run_spark_job(**context):
    import sys
    sys.path.append("/opt/airflow/spark")
    from transform_job import main as run_transform
    run_transform()


with DAG(
    dag_id="transform_weather",
    description="Daily: PySpark job that cleans staging data and rebuilds warehouse tables",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["citypulse", "transform"],
) as dag:

    transform_task = PythonOperator(
        task_id="run_pyspark_transform",
        python_callable=run_spark_job,
    )
