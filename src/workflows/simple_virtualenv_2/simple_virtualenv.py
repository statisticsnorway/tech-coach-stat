from airflow import DAG
from airflow.sdk import task
from datetime import datetime
import pkgutil
import sys

def load_zipped_requirements():
    try:
        # 'requirements.txt' must be in the exact same directory as this script inside the zip
        data = pkgutil.get_data(__name__, "requirements.txt")
        if data:
            lines = data.decode("utf-8").splitlines()
            return [line.strip() for line in lines if line.strip() and not line.startswith("#")]
    except Exception:
        pass
    return []

with DAG(
    dag_id="simple_virtualenv_module_workflow",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    @task.virtualenv(
        task_id="simple_virtualenv", requirements=load_zipped_requirements(), system_site_packages=False
    )
    def callable_virtualenv():
        sys.path.append("/home/airflow/gcs/dags/zip_simple_virtualenv_2.zip/")
        from colormsg.colormsg import my_function
        my_function()

    virtualenv_task = callable_virtualenv()