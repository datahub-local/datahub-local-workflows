from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import V1EnvVar, V1EnvVarSource, V1SecretKeySelector

SQLMESH_IMAGE = "ghcr.io/datahub-local/datahub-local-workflows-sqlmesh:main"
NAMESPACE = "data"

default_args = {
    "owner": "datahub-local",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _secret(secret_name: str, key: str) -> V1EnvVar:
    return V1EnvVar(
        name=key,
        value_from=V1EnvVarSource(
            secret_key_ref=V1SecretKeySelector(name=secret_name, key=key)
        ),
    )


_state_env = [
    _secret("sqlmesh-state-secret", "SQLMESH_STATE_HOST"),
    _secret("sqlmesh-state-secret", "SQLMESH_STATE_USER"),
    _secret("sqlmesh-state-secret", "SQLMESH_STATE_PASSWORD"),
]

_nessie_env = [
    _secret("nessie-secret", "NESSIE_URI"),
    _secret("nessie-secret", "NESSIE_WAREHOUSE"),
    V1EnvVar(name="NESSIE_REF", value="main"),
]

with DAG(
    dag_id="sqlmesh_tests",
    default_args=default_args,
    description="Run all SQLMesh pipelines",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sqlmesh"],
) as dag:

    sqlmesh_pi = KubernetesPodOperator(
        task_id="sqlmesh-pi",
        name="sqlmesh-pi",
        namespace=NAMESPACE,
        image=SQLMESH_IMAGE,
        arguments=["--project", "/app/pi", "run", "--gateway", "spark"],
        env_vars=[
            *_state_env,
            *_nessie_env,
            V1EnvVar(name="PI_PARTITIONS", value="500"),
            V1EnvVar(name="PI_SAMPLES_PER_PARTITION", value="100000"),
            V1EnvVar(name="PI_RANDOM_SEED", value="7"),
        ],
        is_delete_operator_pod=True,
        get_logs=True,
        do_xcom_push=False,
    )

    sqlmesh_example_db = KubernetesPodOperator(
        task_id="sqlmesh-example-db",
        name="sqlmesh-example-db",
        namespace=NAMESPACE,
        image=SQLMESH_IMAGE,
        arguments=["--project", "/app/example_db", "run", "--gateway", "spark"],
        env_vars=[
            *_state_env,
            *_nessie_env,
            _secret("example-db-secret", "EXAMPLE_DB_URL"),
            _secret("example-db-secret", "EXAMPLE_DB_USER"),
            _secret("example-db-secret", "EXAMPLE_DB_PASSWORD"),
            V1EnvVar(name="EXAMPLE_DB_SCHEMA", value="public"),
            V1EnvVar(name="EXAMPLE_DB_DRIVER", value="org.h2.Driver"),
        ],
        is_delete_operator_pod=True,
        get_logs=True,
        do_xcom_push=False,
    )

    sqlmesh_pi >> sqlmesh_example_db
