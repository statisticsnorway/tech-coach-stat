from airflow import DAG
from airflow.sdk import task
from datetime import datetime
import os
import pkgutil

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
    dag_id="simple_virtualenv_workflow",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    @task.virtualenv(
        task_id="simple_virtualenv", requirements=load_zipped_requirements(), system_site_packages=False
    )
    def callable_virtualenv():
        """
        Example function that will be performed in a virtual environment.

        Importing at the module level ensures that it will not attempt to import the
        library before it is installed.
        """
        from time import sleep

        from colorama import Back, Fore, Style

        print(Fore.RED + "some red text")
        print(Back.GREEN + "and with a green background")
        print(Style.DIM + "and in dim text")
        print(Style.RESET_ALL)
        for _ in range(4):
            print(Style.DIM + "Please wait...", flush=True)
            sleep(1)
        print("Finished")

    virtualenv_task = callable_virtualenv()