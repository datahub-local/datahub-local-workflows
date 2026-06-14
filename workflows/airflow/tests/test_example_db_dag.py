import importlib


def test_dag_importable():
    mod = importlib.import_module("dags.example_db_dag")
    assert hasattr(mod, "dag"), "example_db_dag module should expose `dag`"
    dag = mod.dag
    assert dag.dag_id == "example_db"
    assert set(dag.task_ids) == {"dlt_ingest_example_db", "dbt_example_db", "dlt_export_example_db"}


def test_dag_chains_ingest_build_export():
    dag = importlib.import_module("dags.example_db_dag").dag
    assert "dbt_example_db" in dag.get_task("dlt_ingest_example_db").downstream_task_ids
    assert "dlt_export_example_db" in dag.get_task("dbt_example_db").downstream_task_ids


def test_dbt_task_arguments():
    dag = importlib.import_module("dags.example_db_dag").dag
    assert dag.get_task("dbt_example_db").arguments == [
        "--project", "example_db", "--target", "homelab", "--full-refresh",
    ]


def test_dlt_ingest_arguments_and_s3_secrets():
    dag = importlib.import_module("dags.example_db_dag").dag
    ingest = dag.get_task("dlt_ingest_example_db")
    assert ingest.arguments == [
        "--pipeline", "ingest", "--project", "example_db", "--target", "homelab",
    ]
    env_map = {e.name: e for e in ingest.env_vars}
    assert env_map["S3_ACCESS_KEY"].value_from.secret_key_ref.name == "s3-credentials"
    assert env_map["S3_SECRET_KEY"].value_from.secret_key_ref.name == "s3-credentials"


def test_dlt_export_arguments_env_and_secrets():
    dag = importlib.import_module("dags.example_db_dag").dag
    export = dag.get_task("dlt_export_example_db")
    assert export.arguments == [
        "--pipeline", "export", "--project", "example_db", "--target", "homelab",
    ]
    env_map = {e.name: e for e in export.env_vars}
    assert env_map["EXAMPLE_DB_URL"].value == (
        "jdbc:postgresql://datahub-local-core-data-postgresql.data.svc.cluster.local:5432/dbt"
    )
    assert env_map["EXAMPLE_DB_SCHEMA"].value == "dbt_example_db"
    assert env_map["EXAMPLE_DB_USER"].value_from.secret_key_ref.name == "postgresql-admin-credentials"
    assert env_map["S3_ACCESS_KEY"].value_from.secret_key_ref.name == "s3-credentials"
