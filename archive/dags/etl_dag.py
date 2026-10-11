from datetime import datetime, timedelta
import os

import pytz
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils import get_data, transform_data, ingest_data

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("DATASET")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
API_URL = os.getenv("API_URL")

default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_dag_capacity",
    schedule_interval="*/5 * * * *",
    default_args=default_args,
    catchup=False,
) as dag:
    now = datetime.now(pytz.timezone("UTC"))
    current_full_date = now.strftime("%Y-%m-%d %H:%M:00")
    current_ymd = now.strftime("%Y-%m-%d")
    current_hour = now.strftime("%H:00:00")
    current_minute = now.strftime("%H:%M:00")

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=get_data,
        op_kwargs={
            "url": API_URL,
            "bucket_name": GCS_BUCKET_NAME,
            "current_ymd": current_ymd,
            "current_minute": current_minute,
            "mode": "capacity",
        },
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
        op_kwargs={
            "bucket_name": GCS_BUCKET_NAME,
            "current_ymd": current_ymd,
            "current_minute": current_minute,
            "mode": "capacity",
            "timestamp": current_full_date,
        },
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=ingest_data,
        op_kwargs={
            "project_id": GCP_PROJECT_ID,
            "dataset": DATASET,
            "bucket_name": GCS_BUCKET_NAME,
            "current_ymd": current_ymd,
            "current_minute": current_minute,
            "mode": "capacity",
        },
    )

extract_task >> transform_task >> load_task


with DAG(
    "etl_dag_stations",
    schedule_interval="0 6 * * *",
    default_args=default_args,
    catchup=False,
) as dag:
    now = datetime.now(pytz.timezone("UTC"))
    current_full_date = now.strftime("%Y-%m-%d %H:%M:00")
    current_ymd = now.strftime("%Y-%m-%d")
    current_hour = now.strftime("%H:00:00")
    current_minute = now.strftime("%H:%M:00")

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=get_data,
        op_kwargs={
            "url": API_URL,
            "bucket_name": GCS_BUCKET_NAME,
            "current_ymd": current_ymd,
            "current_minute": current_minute,
            "mode": "stations",
        },
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
        op_kwargs={
            "bucket_name": GCS_BUCKET_NAME,
            "current_ymd": current_ymd,
            "current_minute": current_minute,
            "mode": "stations",
            "timestamp": current_full_date,
        },
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=ingest_data,
        op_kwargs={
            "project_id": GCP_PROJECT_ID,
            "dataset": DATASET,
            "bucket_name": GCS_BUCKET_NAME,
            "current_ymd": current_ymd,
            "current_minute": current_minute,
            "mode": "stations",
        },
    )

extract_task >> transform_task >> load_task
