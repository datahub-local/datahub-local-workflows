from datetime import datetime, timedelta

from airflow import DAG

from dags.tasks.sqlmesh_launcher import (
    SQLMeshTaskConfig,
    SecretEnvVarRef,
    create_sqlmesh_task,
)

TEST_EXECUTOR_INSTANCES = 2
TEST_EXECUTOR_MEMORY = "2G"
TEST_DRIVER_CORES = 1
TEST_DRIVER_MEMORY = "2G"
COMMON_ENV_VARS = {
    "SQLMESH_STATE_HOST": "datahub-local-core-data-postgresql:5432",
    "SQLMESH_STATE_PORT": "5432",
    "SQLMESH_STATE_DB": "sqlmesh",
}
EXAMPLE_DB_ENV_VARS = {
    "NESSIE_REF": "main",
    "EXAMPLE_DB_URL": "jdbc:postgresql://datahub-local-core-data-postgresql:5432/sqlmesh",
    "EXAMPLE_DB_SCHEMA": "sqlmesh_example_db",
}
COMMON_SECRET_ENV_VARS = (
    SecretEnvVarRef(secret_name="postgresql-admin-credentials", secret_key="user", env_name="SQLMESH_STATE_USER"),
    SecretEnvVarRef(secret_name="postgresql-admin-credentials", secret_key="password", env_name="SQLMESH_STATE_PASSWORD"),
)
EXAMPLE_DB_SECRET_ENV_VARS = (
    *COMMON_SECRET_ENV_VARS,
    SecretEnvVarRef(secret_name="example-db-secret", secret_key="user", env_name="EXAMPLE_DB_USER"),
    SecretEnvVarRef(secret_name="example-db-secret", secret_key="password", env_name="EXAMPLE_DB_PASSWORD"),
)

default_args = {
    "owner": "datahub-local",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="sqlmesh",
    default_args=default_args,
    description="Run SQLMesh test pipelines in Kubernetes",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sqlmesh"],
) as dag:
    sqlmesh_pi = create_sqlmesh_task(
        SQLMeshTaskConfig(
            task_id="sqlmesh_pi",
            pipeline_name="pi",
            env_vars=COMMON_ENV_VARS,
            secret_env_vars=COMMON_SECRET_ENV_VARS,
            executor_instances=TEST_EXECUTOR_INSTANCES,
            executor_memory=TEST_EXECUTOR_MEMORY,
            driver_cores=TEST_DRIVER_CORES,
            driver_memory=TEST_DRIVER_MEMORY,
        )
    )

    sqlmesh_example_db = create_sqlmesh_task(
        SQLMeshTaskConfig(
            task_id="sqlmesh_example_db",
            pipeline_name="example_db",
            env_vars=COMMON_ENV_VARS,
            secret_env_vars=EXAMPLE_DB_SECRET_ENV_VARS,
            executor_instances=TEST_EXECUTOR_INSTANCES,
            executor_memory=TEST_EXECUTOR_MEMORY,
            driver_cores=TEST_DRIVER_CORES,
            driver_memory=TEST_DRIVER_MEMORY,
        )
    )

    sqlmesh_pi >> sqlmesh_example_db
