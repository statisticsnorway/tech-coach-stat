import datetime

from airflow import models
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with models.DAG(
    dag_id="test_kubernetes_pod_operator",
    schedule=None,
    start_date=datetime.datetime(2024, 1, 1),
) as dag:
  KubernetesPodOperator(
    task_id='task_1',
    image='ghcr.io/statisticsnorway/docker-rpython-base:latest',
    cmds=['python3'],
    arguments=[
        '--version'
    ],
    get_logs=True,
  )