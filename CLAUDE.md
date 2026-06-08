# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository overview

A monorepo of data workflow definitions for a local/homelab Datahub stack. Three separate sub-projects, each with their own `pyproject.toml` and `uv` environment:

- `workflows/sqlmesh/` — SQLMesh pipelines (Spark + Iceberg + Nessie, medallion architecture)
- `workflows/airflow/` — Airflow DAGs that orchestrate SQLMesh runs via Kubernetes pods
- `n8n/` — n8n workflow JSON exports and prompts (no Python code)

## Commands

Each sub-project uses `uv`. Run commands from the sub-project directory.

### SQLMesh (`workflows/sqlmesh/`)

```bash
# Install
uv pip install -e ".[dev]"

# Run pipeline (local gateway — no external services)
SQLMESH_LOCAL_GATEWAY=true uv run sqlmesh -p pipelines/example_db plan
SQLMESH_LOCAL_GATEWAY=true uv run sqlmesh -p pipelines/example_db run

# Run pipeline (homelab gateway — Kubernetes/Nessie/PostgreSQL required)
uv run sqlmesh -p pipelines/example_db --gateway homelab plan
uv run sqlmesh -p pipelines/pi --gateway homelab run

# Tests (no Spark required — config/model metadata only)
uv run pytest tests/
uv run pytest tests/example_db/test_config.py   # single test file
uv run pytest tests/example_db/test_models.py::TestAutomotiveSourceModel::test_model_name  # single test

# Lint
uv run ruff check .
```

### Airflow (`workflows/airflow/`)

```bash
uv sync
uv run pytest          # all tests
uv run pytest tests/tasks/test_sqlmesh_utils.py  # single file
uv run ruff check .
```

## Architecture

### SQLMesh pipelines

Each pipeline lives under `pipelines/<name>/` with a `config.yaml` and a `models/` tree. Two gateways are defined in every `config.yaml`:

| Gateway | Engine | State store | When to use |
|---------|--------|-------------|-------------|
| `homelab` (default) | PySpark on Kubernetes | PostgreSQL | Production / CI |
| `local` | Spark local mode | DuckDB (`local.db`) | Local dev — no external infra |

The `local` gateway is only emitted by Jinja when `SQLMESH_LOCAL_GATEWAY=true`; it is absent from the parsed config by default.

**`example_db` pipeline** follows the medallion pattern with three Iceberg catalogs:
- `nessie_bronze` — raw ingestion (`automotive_source`, `automotive_snapshot`)
- `nessie_silver` — cleaned/typed data (`automotive_raw`)
- `nessie_gold` — aggregated summaries (`automotive_make_price_summary`, `automotive_fuel_body_mpg_summary`)

Models that belong to a non-default catalog must prefix their `@model` name with the catalog (e.g. `nessie_silver.example_db.automotive_raw`). Models with no catalog prefix resolve to `nessie_bronze` (the `spark.sql.defaultCatalog`).

**`pi` pipeline** is a Monte Carlo π estimator; tunable via `PI_PARTITIONS`, `PI_SAMPLES_PER_PARTITION`, `PI_RANDOM_SEED`.

#### Local gateway catalog constraint

The local gateway uses `JdbcCatalog` (H2 in-memory) rather than `HadoopCatalog`. This is intentional: `HadoopCatalog` throws `UnsupportedOperationException` when SQLMesh tries to create views during `plan`. Every catalog entry in the local gateway config must include `jdbc.schema-version: V1` — omitting it disables view support.

#### SQLMesh model name qualification

SQLMesh qualifies unqualified model names with the default catalog and wraps identifiers in double quotes internally. When iterating `ctx.models`, keys look like `'"nessie_bronze"."example_db"."automotive_source"'`. Strip quotes with `name.replace('"', '')` to get `nessie_bronze.example_db.automotive_source`.

### Airflow DAG and SQLMesh launcher

`workflows/airflow/dags/sqlmesh_dag.py` defines a single DAG (`sqlmesh`) that runs SQLMesh pipelines as Kubernetes pods via `KubernetesPodOperator`.

The helper in `dags/tasks/sqlmesh_utils.py` wraps the operator:
- `SQLMeshTaskConfig` — frozen dataclass, all pod configuration lives here
- `create_sqlmesh_task()` — builds the `KubernetesPodOperator` from a config
- Kubernetes secrets (`SecretEnvVarRef`) and ConfigMap refs are resolved at pod launch time, not at DAG parse time
- Pod metadata fields (`SPARK_NAMESPACE`, `SPARK_DRIVER_POD_NAME`, `SPARK_DRIVER_POD_IP`) are injected via `fieldRef` so the Spark driver can advertise its pod IP back to executors

DAG execution order: `sqlmesh_migrate → sqlmesh_pi → sqlmesh_example_db`

The Docker image used for SQLMesh pods is `ghcr.io/datahub-local/datahub-local-workflows-sqlmesh:main`.

### Test structure

SQLMesh tests in `tests/` are intentionally lightweight — they validate config YAML and model metadata (via AST parsing) without starting a Spark session. The `conftest.py` in `tests/example_db/` adds the pipeline directory to `sys.path` and sets JDBC env var defaults so model imports resolve without real credentials.

`tests/example_db/test_integration.py` is the only test that requires a live Spark session (tagged separately — skip it for fast iteration).
