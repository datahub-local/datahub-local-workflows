################################
# Adjust path for local imports
###############################
import os
import sys

SCRIPT_DIR = os.path.realpath(os.path.dirname(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
print(f"Added {os.path.dirname(SCRIPT_DIR)} to sys.path")
###############################

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import task

from dags.tasks.spark_utils import run_spark_app

default_args = {
    "owner": "datahub-local",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_sdp_tests",
    default_args=default_args,
    description="Run Spark SDP jobs",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    @task(task_id="spark-pi")
    def spark_pi(**context):
        app_name, success = run_spark_app(
            source_app_name="spark-sdp", parameters={"pipeline": "pi"}
        )

        result = {
            "app_name": app_name,
            "success": success,
        }

        if not success:
            raise RuntimeError(
                f"SparkApplication '{app_name}' failed. Check logs for details."
            )

        return result

    @task(task_id="spark-example-db")
    def spark_example_db(**context):
        app_name, success = run_spark_app(
            source_app_name="spark-sdp", parameters={"pipeline": "example-db"}
        )

        result = {
            "app_name": app_name,
            "success": success,
        }

        if not success:
            raise RuntimeError(
                f"SparkApplication '{app_name}' failed. Check logs for details."
            )

        return result

    spark_pi() >> spark_example_db()
