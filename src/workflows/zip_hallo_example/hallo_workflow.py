from datetime import datetime
from airflow import DAG
from airflow.sdk import task
with DAG(
    dag_id="hallo_zip_workflow",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    @task(task_id="my_task")
    def my_task():
        print("Hello from Airflow zipped workflow!")
    task_1 = my_task()