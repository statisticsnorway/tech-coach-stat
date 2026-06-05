from airflow import DAG
from airflow.sdk import task
from notebooks import c_pre_inndata_to_inndata
from datetime import datetime

with DAG(
    dag_id="zip_workflow",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    @task.virtualenv(task_id="my_task", requirements="requirements.txt", system_site_packages=False)
    def my_task():
        c_pre_inndata_to_inndata.run_all()
    task_1 = my_task()
