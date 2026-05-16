import importlib


def test_dag_importable():
    mod = importlib.import_module("dags.sqlmesh_dag")
    assert hasattr(mod, "dag"), "sqlmesh_dag module should expose `dag`"
    dag = mod.dag
    assert dag.dag_id == "sqlmesh"
    assert set(dag.task_ids) == {"sqlmesh_pi", "sqlmesh_example_db"}


def test_sqlmesh_test_dag_runs_pi_before_example_db():
    mod = importlib.import_module("dags.sqlmesh_dag")
    dag = mod.dag

    sqlmesh_pi = dag.get_task("sqlmesh_pi")
    sqlmesh_example_db = dag.get_task("sqlmesh_example_db")

    assert "sqlmesh_example_db" in sqlmesh_pi.downstream_task_ids
    assert "sqlmesh_pi" in sqlmesh_example_db.upstream_task_ids


def test_sqlmesh_test_dag_tasks_target_expected_pipelines():
    mod = importlib.import_module("dags.sqlmesh_dag")
    dag = mod.dag

    sqlmesh_pi = dag.get_task("sqlmesh_pi")
    sqlmesh_example_db = dag.get_task("sqlmesh_example_db")

    assert sqlmesh_pi.arguments == [
        "--paths",
        "/app/pipelines/pi",
        "run",
        "--gateway",
        "spark",
    ]


def test_sqlmesh_test_dag_passes_example_db_env_from_secrets_from_dag():
    mod = importlib.import_module("dags.sqlmesh_dag")
    dag = mod.dag

    sqlmesh_pi = dag.get_task("sqlmesh_pi")
    sqlmesh_example_db = dag.get_task("sqlmesh_example_db")
    pi_env_var_map = {env_var.name: env_var for env_var in sqlmesh_pi.env_vars}
    example_db_env_var_map = {
        env_var.name: env_var for env_var in sqlmesh_example_db.env_vars
    }

    assert pi_env_var_map["NESSIE_URI"].value_from.secret_key_ref.name == "nessie-secret"
    assert pi_env_var_map["SQLMESH_STATE_HOST"].value_from.secret_key_ref.name == "sqlmesh-state-secret"
    assert pi_env_var_map["SQLMESH_STATE_DB"].value_from.secret_key_ref.name == "postgres-db-secret"
    assert example_db_env_var_map["EXAMPLE_DB_URL"].value_from.secret_key_ref.name == "example-db-secret"
    assert example_db_env_var_map["EXAMPLE_DB_USER"].value_from.secret_key_ref.name == "example-db-secret"
    assert example_db_env_var_map["EXAMPLE_DB_PASSWORD"].value_from.secret_key_ref.name == "example-db-secret"
    assert example_db_env_var_map["NESSIE_REF"].value == "main"
    assert sqlmesh_example_db.arguments == [
        "--paths",
        "/app/pipelines/example_db",
        "run",
        "--gateway",
        "spark",
    ]