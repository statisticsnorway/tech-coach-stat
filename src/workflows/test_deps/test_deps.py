from datetime import datetime
from airflow import DAG
from airflow.sdk import task
from urllib.request import urlretrieve

def load_requirements():
    urlretrieve("https://gist.githubusercontent.com/heidisu/98c0bfc8125b0fa4222d907b332b57a1/raw/c289bb70499cbef6d3979e3f2bf5626ec1c50c11/requirements.txt", "requirements.txt")
    with open("requirements.txt") as f:
        requirements =  [line.strip() for line in f if line.strip()]
        requirements.append( "tech-coach-stat @ git+https://github.com/statisticsnorway/tech-coach-stat.git@package-notebooks")
        return requirements


with DAG(
    dag_id="test_dependencies",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    @task.virtualenv(
        task_id="c_pre_inndata_to_inndata", 
        requirements= load_requirements(), 
        system_site_packages=False
    )
    def task_c():
        from notebooks import c_pre_inndata_to_inndata
        c_pre_inndata_to_inndata.run_all()

    @task.virtualenv(
        task_id="d_prepare_edit", 
        requirements= load_requirements(), 
        system_site_packages=False
    )
    def task_d():
        from notebooks import d_prepare_edit
        d_prepare_edit.run_all()
    task_c() >> task_d()