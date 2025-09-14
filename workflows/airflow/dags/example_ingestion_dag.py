from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from dags.lib.sample_tasks import fetch_sample, process_sample


default_args = {
    "owner": "datahub-local",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="example_ingestion",
    default_args=default_args,
    description="Example ingestion DAG: raw -> bronze",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    @task(task_id="fetch_sample")
    def _fetch():
        return fetch_sample({})

    @task(task_id="process_sample")
    def _process():
        return process_sample({})

    _fetch() >> _process()
