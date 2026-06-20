import importlib


def _dag():
    return importlib.import_module("dags.bodega_dag").dag


def test_dag_importable():
    dag = _dag()
    assert dag.dag_id == "bodega_daily"
    assert set(dag.task_ids) == {
        "n8n_download_invoices",
        "dlt_ingest_bodega",
        "dbt_silver_bodega",
        "dlt_enrich_bodega",
        "dbt_gold_bodega",
    }


def test_dag_schedule():
    assert _dag().schedule == "0 8 * * *"


def test_task_ordering():
    dag = _dag()
    assert "dlt_ingest_bodega" in dag.get_task("n8n_download_invoices").downstream_task_ids
    assert "dbt_silver_bodega" in dag.get_task("dlt_ingest_bodega").downstream_task_ids
    assert "dlt_enrich_bodega" in dag.get_task("dbt_silver_bodega").downstream_task_ids
    assert "dbt_gold_bodega" in dag.get_task("dlt_enrich_bodega").downstream_task_ids


def test_n8n_download_invoices():
    mod = importlib.import_module("dags.bodega_dag")
    task = mod.dag.get_task("n8n_download_invoices")
    assert task.op_kwargs["workflow_name"] == mod.N8N_DOWNLOAD_INVOICES_WORKFLOW_NAME
    assert task.op_kwargs["workflow_name"] == "DownloadInvoicesFromGmail"
    assert task.op_kwargs["params"] == {"FROM_DATE": "{{ data_interval_start | ds }}", "TO_DATE": "{{ macros.ds_add(data_interval_end | ds, +1) }}"}


def test_dlt_ingest_arguments():
    ingest = _dag().get_task("dlt_ingest_bodega")
    assert ingest.arguments == [
        "--pipeline", "ingest", "--project", "bodega", "--target", "homelab",
    ]


def test_dlt_enrich_arguments():
    enrich = _dag().get_task("dlt_enrich_bodega")
    assert enrich.arguments == [
        "--pipeline", "enrich", "--project", "bodega", "--target", "homelab",
    ]


def test_dbt_silver_arguments():
    task = _dag().get_task("dbt_silver_bodega")
    assert task.arguments == [
        "--project", "bodega", "--target", "homelab", "--select", "silver.*",
    ]


def test_dbt_gold_arguments():
    task = _dag().get_task("dbt_gold_bodega")
    assert task.arguments == [
        "--project", "bodega", "--target", "homelab", "--select", "gold.*",
    ]


def test_ingest_kafka_env_vars():
    ingest = _dag().get_task("dlt_ingest_bodega")
    env_map = {e.name: e for e in ingest.env_vars}
    assert "KAFKA_BOOTSTRAP_SERVERS" in env_map
    assert "redpanda" in env_map["KAFKA_BOOTSTRAP_SERVERS"].value
    assert env_map["KAFKA_TOPIC_BODEGA"].value == "bodega_invoices"


def test_ingest_s3_secrets():
    ingest = _dag().get_task("dlt_ingest_bodega")
    env_map = {e.name: e for e in ingest.env_vars}
    assert env_map["S3_ACCESS_KEY"].value_from.secret_key_ref.name == "s3-credentials"
    assert env_map["S3_SECRET_KEY"].value_from.secret_key_ref.name == "s3-credentials"


def test_enrich_s3_secrets():
    enrich = _dag().get_task("dlt_enrich_bodega")
    env_map = {e.name: e for e in enrich.env_vars}
    assert env_map["S3_ACCESS_KEY"].value_from.secret_key_ref.name == "s3-credentials"
    assert env_map["S3_SECRET_KEY"].value_from.secret_key_ref.name == "s3-credentials"
