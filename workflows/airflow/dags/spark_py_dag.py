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
from airflow.decorators import task
from dags.tasks.spark_utils import clone_and_wait_for_spark_app


default_args = {
    "owner": "datahub-local",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_py",
    default_args=default_args,
    description="Run Python Spark job with random cloned SparkApplication",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    @task(task_id="clone_and_run_spark_job")
    def run_spark_job(**context):
        """Clone the python-py SparkApplication and wait for it to complete.

        This task:
        1. Clones the 'python-py' SparkApplication with a random name
        2. Waits for the cloned application to complete (up to 1 hour)
        3. Returns success/failure status

        Args:
            **context: Airflow task context (provides task instance metadata)

        Returns:
            dict with 'app_name' and 'success' keys
        """
        app_name, success = clone_and_wait_for_spark_app(
            source_app_name="python-py",
            source_namespace="default",
            target_namespace="default",
            new_app_name=None,  # Auto-generate a random name
            timeout_seconds=3600,  # 1 hour timeout
            poll_interval_seconds=10,  # Check every 10 seconds
            in_cluster=True,  # Run from within Kubernetes cluster
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

    run_spark_job()
