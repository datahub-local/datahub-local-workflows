# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository overview

A monorepo of data workflow definitions for a local/homelab Datahub stack. Four separate sub-projects, each with their own `pyproject.toml` and `uv` environment:

- `workflows/dbt/` — dbt Core pipelines on **Trino** (homelab) / **DuckDB** (local), Iceberg + Apache Polaris, medallion architecture; a thin Python `dbt_runner` wraps `dbt build`
- `workflows/dlt/` — [dlt](https://dlthub.com) ingest/export pipelines (CSV → bronze; silver/gold → Postgres) that run *around* dbt
- `workflows/airflow/` — Airflow DAGs that orchestrate the dlt + dbt tasks via Kubernetes pods
- `n8n/` — n8n workflow JSON exports and prompts (no Python code)

## Naming conventions

Sub-projects follow a consistent structure:

```
workflows/<tool>/
  <tool>_runner/          Python entry point package (python -m <tool>_runner)
  <pipeline>/             dbt project or named pipeline (e.g. example_db, pi)
  tests/
    <pipeline>/           tests grouped by pipeline (integration tests)
    test_<cross>.py       cross-pipeline tests stay at the tests/ root
  Dockerfile
  pyproject.toml          name: datahub-local-workflows-<tool>
  README.md
```

| What | Convention | Examples |
|---|---|---|
| Package name | `datahub-local-workflows-<tool>` | `datahub-local-workflows-dbt` |
| Python runner module | `<tool>_runner` | `dbt_runner`, `dlt_runner` |
| Container entry point | `python -m <tool>_runner` | `ENTRYPOINT ["python", "-m", "dbt_runner"]` |
| Pipeline / project dir | lowercase, underscores | `example_db`, `pi` |
| Test subdirs | per-pipeline, mirroring source | `tests/example_db/`, `tests/pi/` |

## Commands

Each sub-project uses `uv`. Run commands from the sub-project directory.

### dbt (`workflows/dbt/`)

dbt runs SQL directly on Trino (homelab) or DuckDB (local). The `dbt_runner`
package is just a convenience wrapper around `dbt build`.

```bash
# Install
uv sync --extra dev

# Run pipeline (local target — DuckDB, no external services)
uv run python -m dbt_runner --project pi --target local
uv run python -m dbt_runner --project example_db --target local   # needs bronze source seeded first

# Run pipeline (homelab target — Trino/Polaris/S3 required)
uv run python -m dbt_runner --project example_db --target homelab

# Validate a project without a warehouse connection
uv run dbt parse --project-dir pi --profiles-dir pi --target local

# Tests (no warehouse — project structure, model SQL, dbt parse)
uv run pytest tests/ --ignore=tests/example_db/test_integration.py
# DuckDB end-to-end (seeds bronze, runs dbt build, asserts medallion tables)
uv run pytest tests/example_db/test_integration.py

uv run ruff check .
```

### dlt (`workflows/dlt/`)

```bash
uv sync --extra dev
# Full local chain (DuckDB), shared warehouse dir with dbt:
export DBT_DUCKDB_DIR=/tmp/duckdb
uv run python -m dlt_runner --pipeline ingest --project example_db --target local
uv run python -m dlt_runner --pipeline export --project example_db --target local
uv run pytest
uv run ruff check .
```

### Airflow (`workflows/airflow/`)

```bash
uv sync
uv run pytest          # all tests
uv run pytest tests/tasks/test_dlt.py  # single file
```

## Architecture

### Engines and targets

dbt models run identical `database.schema.table` SQL on both engines:

| Target | dbt engine | dlt ingest / export | When to use |
|--------|------------|---------------------|-------------|
| `homelab` (default) | Trino on Kubernetes (Iceberg over Apache Polaris REST + S3) | Iceberg via Apache Polaris REST / Postgres | Production / CI |
| `local` | DuckDB (one file per catalog) | DuckDB files / DuckDB export file | Local dev — no external infra |

dbt is **stateless** — no plan/apply or state store. All models are `materialized='table'`.

### dbt pipelines and the runner

Each pipeline is a self-contained dbt project under `workflows/dbt/<name>/` (`dbt_project.yml`, `profiles.yml`, `models/`). `dbt_runner/__main__.py` (`--project --target [--select] [--full-refresh]`) just resolves the project dir and invokes `dbt build` in-process. It does **not** ingest or export — those are dlt pipelines.

- **`example_db`** follows the medallion pattern with three catalogs (`bronze`/`silver`/`gold`).
- **`pi`** is a Monte Carlo π estimator tunable via `PI_PARTITIONS`, `PI_SAMPLES_PER_PARTITION`, `PI_RANDOM_SEED` (dbt `vars`). Row generation differs between DuckDB and Trino, so `pi/macros/generate_samples.sql` is **adapter-dispatched** (`duckdb__` uses `range()`, `trino__` cross-joins `sequence()` unnests); both draw x/y with `random()` (the seed is carried as a column but engine `random()` is unseeded).

#### Medallion catalogs

Trino and DuckDB both have real catalogs, so each medallion layer is its own catalog. Models set `+database` (catalog) and `+schema` (`example_db`) in `dbt_project.yml`, yielding `<catalog>.example_db.<table>`. The `generate_schema_name` macro (example_db) keeps the schema verbatim instead of the dbt default `<target>_<custom>` concatenation. `pi` has no `generate_schema_name` macro and relies on the profile's `database`/`schema`.

- **homelab (Trino):** the Trino server defines one Iceberg catalog per Apache Polaris catalog — `bronze`, `silver`, `gold`, `test` (used by `pi`). Catalogs are provisioned in Polaris; dbt does not create them.
- **local (DuckDB):** one DuckDB file per catalog (`bronze.duckdb`, …). dbt-duckdb derives the catalog name from the **file basename**, so file names must match catalog names. The example_db local profile attaches `silver`/`gold`; paths come from `DBT_DUCKDB_*` env vars (shared with dlt).

### dlt ingest/export (`workflows/dlt/`)

`dlt_runner/__main__.py` (`--pipeline {ingest,export} --project example_db --target {homelab,local}`) dispatches to the pipeline. Pipeline code lives in `example_db/`:

- `example_db/ingest.py` — streams the automotive CSV into `bronze.example_db.automotive_source` (the dbt source). The `direct` naming convention preserves the raw hyphenated column names. homelab writes Iceberg via pyiceberg's REST catalog (`config.configure_iceberg_env` sets `PYICEBERG_CATALOG__<LAYER>__*`), staging parquet in the **temp bucket** (`datahub-local-temp`); local writes DuckDB.
- `example_db/export.py` — reads the dbt-built silver/gold tables (Trino on homelab, DuckDB locally) and loads them to Postgres (homelab) / a DuckDB export file (local).
- `example_db/config.py` — env-driven: catalog→warehouse map, temp bucket, DuckDB paths (reusing the dbt `DBT_DUCKDB_*` vars so a local ingest lands in the files dbt reads), Postgres/Trino DSNs.

### Airflow DAGs and the launchers

**DAGs are one file per pipeline** — each is self-contained and runs as Kubernetes pods:

- `dags/pi_dag.py` (`dag_id: pi`) — pure dbt compute: `dbt_pi`
- `dags/example_db_dag.py` (`dag_id: example_db`) — full medallion pipeline: `dlt_ingest_example_db → dbt_example_db → dlt_export_example_db`

**Task utilities are organised by tool** under `dags/tasks/`:

- `dags/tasks/dbt.py` — `DbtTaskConfig` + `create_dbt_task` (dbt image, `python -m dbt_runner`). The shared `build_pod_env_vars` / `build_pod_resources` and the `SecretEnvVarRef`/`ConfigMapEnvVarRef` dataclasses live here. Pod env is just the explicit env/secret/configmap refs — the pods only need to connect to Trino/Postgres/S3.
- `dags/tasks/dlt.py` — `DltTaskConfig` + `create_dlt_task` (dlt image, `python -m dlt_runner`), reusing the dbt builders.
- Images: `ghcr.io/datahub-local/datahub-local-workflows-dbt:main` and `...-dlt:main`.

### Test structure

- dbt `tests/` are lightweight: project/profile structure, model SQL, and `dbt parse` — no warehouse. `tests/example_db/test_integration.py` seeds the bronze source in DuckDB, runs `dbt build` **in a subprocess** (avoids DuckDB's per-process "file already attached" conflict), and asserts the medallion tables.
- dlt `tests/example_db/` covers the local DuckDB ingest+export integration test; `tests/test_config.py` covers `example_db/config.py` helpers (cross-pipeline, stays at the tests root).
- airflow `tests/` import the DAGs and check the dbt/dlt task arguments, env, and ordering — `test_pi_dag.py` covers the pi DAG; `test_example_db_dag.py` covers example_db.
