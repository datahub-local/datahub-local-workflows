import importlib


def test_dag_importable():
    mod = importlib.import_module("dags.pi_dag")
    assert hasattr(mod, "dag"), "pi_dag module should expose `dag`"
    dag = mod.dag
    assert dag.dag_id == "pi"
    assert set(dag.task_ids) == {"dbt_pi"}


def test_dbt_pi_arguments():
    dag = importlib.import_module("dags.pi_dag").dag
    assert dag.get_task("dbt_pi").arguments == [
        "--project", "pi", "--target", "homelab", "--full-refresh",
    ]
