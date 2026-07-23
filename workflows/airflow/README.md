# Airflow DAGs

Airflow DAGs that orchestrate the dbt and dlt pipelines via Kubernetes pods. Each task runs
the corresponding image (`datahub-local-ai-dbt` or `datahub-local-ai-dlt`) as a
`KubernetesPodOperator` in the `data` namespace.

## Structure

```
workflows/airflow/
  dags/
    pi_dag.py           DAG for the pi pipeline (pure dbt compute)
    example_db_dag.py   DAG for example_db (dlt ingest → dbt build → dlt export)
    tasks/
      dbt.py      DbtTaskConfig + create_dbt_task; shared pod env/resource builders
      dlt.py      DltTaskConfig + create_dlt_task; reuses dbt builders
  tests/
    tasks/              unit tests for dbt and dlt
    test_pi_dag.py      DAG structure and task arguments for pi
    test_example_db_dag.py  DAG structure, task ordering, and arguments for example_db
  pyproject.toml        package: datahub-local-ai-airflow
```

**DAGs are one file per pipeline** — each file is self-contained and simple to read.
**Task utilities are organised by tool** under `tasks/` (`dbt.py` for dbt pods,
`dlt.py` for dlt pods). `dlt` reuses the shared pod builders from `dbt`.

## DAGs

### `pi_dag` — `dag_id: pi`

Pure dbt compute; no ingest or export.

```
dbt_pi
```

### `example_db_dag` — `dag_id: example_db`

Full medallion pipeline: CSV ingest → dbt build → Postgres export.

```
dlt_ingest_example_db → dbt_example_db → dlt_export_example_db
```

Each task is a `KubernetesPodOperator` that invokes:
- dbt tasks: `python -m dbt_runner --project <name> --target homelab`
- dlt tasks: `python -m dlt_runner --pipeline <ingest|export> --project <name> --target homelab`

## How to use it

```bash
cd workflows/airflow
uv sync

# Run all tests
uv run pytest

# Run a specific test file
uv run pytest tests/tasks/test_dbt.py -v
uv run pytest tests/test_pi_dag.py -v
uv run pytest tests/test_example_db_dag.py -v

uv run ruff check .
```

## Tests

Tests import DAGs and task utilities directly — no Airflow scheduler or database required.
They verify task arguments, environment variable injection, task ordering, and pod config.

## Environment variables injected into pods

Pod environment comes from explicit `env_vars`, `secret_env_vars` (K8s Secrets), and
`configmap_env_vars` (K8s ConfigMaps) declared in each `DbtTaskConfig` / `DltTaskConfig`.
The pods only need credentials to reach Trino, Postgres, and S3 — they do not inherit the
Airflow worker environment.

See [`../dbt/README.md`](../dbt/README.md) and [`../dlt/README.md`](../dlt/README.md) for the
full list of env variables each image expects.
