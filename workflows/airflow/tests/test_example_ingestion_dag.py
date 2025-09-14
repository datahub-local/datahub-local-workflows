import importlib


def test_dag_importable():
    mod = importlib.import_module("dags.example_ingestion_dag")
    assert hasattr(mod, "dag"), "example_ingestion_dag module should expose `dag`"
    dag = mod.dag
    assert dag.dag_id == "example_ingestion"


def test_tasks_and_dependencies():
    mod = importlib.import_module("dags.example_ingestion_dag")
    dag = mod.dag

    expected = {"fetch_sample", "process_sample"}
    assert expected.issubset(set(dag.task_ids))

    fetch = dag.get_task("fetch_sample")
    process = dag.get_task("process_sample")

    # Check fetch -> process ordering
    assert "process_sample" in fetch.downstream_task_ids
    assert "fetch_sample" in process.upstream_task_ids
