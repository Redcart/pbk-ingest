from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def my_callable(*args, **kwargs):
    print("Hello from PythonOperator")


with DAG(
    "my_dag",
    schedule_interval="*/10 * * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    python_task = PythonOperator(task_id="my_python_task", python_callable=my_callable)
