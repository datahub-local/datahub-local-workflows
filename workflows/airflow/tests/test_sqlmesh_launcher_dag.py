import importlib


def test_dag_importable():
    mod = importlib.import_module("dags.sqlmesh_dag")
    assert hasattr(mod, "dag"), "sqlmesh_dag module should expose `dag`"
    dag = mod.dag
    assert dag.dag_id == "sqlmesh"
    assert set(dag.task_ids) == {"sqlmesh_migrate", "sqlmesh_pi", "sqlmesh_example_db"}


def test_sqlmesh_test_dag_runs_migrate_then_pi_then_example_db():
    mod = importlib.import_module("dags.sqlmesh_dag")
    dag = mod.dag

    sqlmesh_migrate = dag.get_task("sqlmesh_migrate")
    sqlmesh_pi = dag.get_task("sqlmesh_pi")
    sqlmesh_example_db = dag.get_task("sqlmesh_example_db")

    assert "sqlmesh_pi" in sqlmesh_migrate.downstream_task_ids
    assert "sqlmesh_migrate" in sqlmesh_pi.upstream_task_ids
    assert "sqlmesh_example_db" in sqlmesh_pi.downstream_task_ids
    assert "sqlmesh_pi" in sqlmesh_example_db.upstream_task_ids


def test_sqlmesh_test_dag_tasks_target_expected_pipelines():
    mod = importlib.import_module("dags.sqlmesh_dag")
    dag = mod.dag

    sqlmesh_migrate = dag.get_task("sqlmesh_migrate")
    sqlmesh_pi = dag.get_task("sqlmesh_pi")
    sqlmesh_example_db = dag.get_task("sqlmesh_example_db")

    assert sqlmesh_migrate.arguments == [
        "-p",
        "/app/pipelines/example_db",
        "--gateway",
        "homelab",
        "migrate",
    ]
    assert sqlmesh_pi.arguments == [
        "-p",
        "/app/pipelines/pi",
        "--gateway",
        "homelab",
        "plan",
        "--auto-apply",
        "--no-prompts",
        "--restate-model",
        "*",
        "prod",
    ]
    assert sqlmesh_example_db.arguments == [
        "-p",
        "/app/pipelines/example_db",
        "--gateway",
        "homelab",
        "plan",
        "--auto-apply",
        "--no-prompts",
        "--restate-model",
        "*",
        "prod",
    ]


def test_sqlmesh_test_dag_passes_expected_env_from_dag():
    mod = importlib.import_module("dags.sqlmesh_dag")
    dag = mod.dag

    sqlmesh_pi = dag.get_task("sqlmesh_pi")
    sqlmesh_example_db = dag.get_task("sqlmesh_example_db")
    pi_env_var_map = {env_var.name: env_var for env_var in sqlmesh_pi.env_vars}
    example_db_env_var_map = {
        env_var.name: env_var for env_var in sqlmesh_example_db.env_vars
    }

    assert pi_env_var_map["SQLMESH_STATE_HOST"].value == "datahub-local-core-data-postgresql.data.svc.cluster.local"
    assert pi_env_var_map["SQLMESH_STATE_PORT"].value == "5432"
    assert pi_env_var_map["SQLMESH_STATE_DB"].value == "sqlmesh"
    assert pi_env_var_map["SQLMESH_STATE_USER"].value_from.secret_key_ref.name == "postgresql-admin-credentials"
    assert pi_env_var_map["SQLMESH_STATE_PASSWORD"].value_from.secret_key_ref.name == "postgresql-admin-credentials"
    assert pi_env_var_map["SPARK_NAMESPACE"].value_from.field_ref.field_path == "metadata.namespace"
    assert pi_env_var_map["SPARK_DRIVER_POD_NAME"].value_from.field_ref.field_path == "metadata.name"
    assert pi_env_var_map["SPARK_DRIVER_POD_IP"].value_from.field_ref.field_path == "status.podIP"
    assert example_db_env_var_map["EXAMPLE_DB_URL"].value == "jdbc:postgresql://datahub-local-core-data-postgresql.data.svc.cluster.local:5432/sqlmesh"
    assert example_db_env_var_map["EXAMPLE_DB_SCHEMA"].value == "sqlmesh_example_db"
    assert example_db_env_var_map["EXAMPLE_DB_USER"].value_from.secret_key_ref.name == "postgresql-admin-credentials"
    assert example_db_env_var_map["EXAMPLE_DB_PASSWORD"].value_from.secret_key_ref.name == "postgresql-admin-credentials"
    assert example_db_env_var_map["NESSIE_REF"].value == "main"
    assert example_db_env_var_map["SPARK_NAMESPACE"].value_from.field_ref.field_path == "metadata.namespace"
    assert example_db_env_var_map["SPARK_DRIVER_POD_NAME"].value_from.field_ref.field_path == "metadata.name"
    assert example_db_env_var_map["SPARK_DRIVER_POD_IP"].value_from.field_ref.field_path == "status.podIP"