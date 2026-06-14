# dlt Pipelines

[dlt](https://dlthub.com) ingest/export pipelines for `datahub-local-workflows`. Handles the
data movement *around* dbt — loading source data into bronze and exporting dbt-built tables.

- **ingest** — loads the automotive CSV into `bronze.example_db.automotive_source`, which dbt reads as a source.
- **export** — reverse-ETLs the dbt-built silver/gold tables to Postgres (homelab) or a DuckDB file (local).

Only `example_db` has ingest/export; `pi` is pure dbt compute.

## Contents

```
workflows/dlt/
  dlt_runner/         entry point: python -m dlt_runner --pipeline <ingest|export> --project <name> --target <env>
    __main__.py       CLI argument parsing and pipeline dispatch
  example_db/         automotive dataset pipeline
    config.py         env-driven config (catalog→warehouse map, DuckDB paths, DSNs)
    ingest.py         CSV → bronze medallion layer
    export.py         silver/gold → Postgres or DuckDB export file
  tests/
    example_db/       integration test (local DuckDB ingest + export end-to-end)
    test_config.py    config unit tests
  Dockerfile
  pyproject.toml      package: datahub-local-workflows-dlt
```

## Pipelines

| Pipeline | Ingest source | Export destination | Notes |
|---|---|---|---|
| `example_db` | Automotive CSV (URL or local file) | Postgres / DuckDB export file | Only pipeline with ingest+export |

## Targets

| Target | Ingest destination | Export destination | Staging |
|---|---|---|---|
| `homelab` (default) | Iceberg via Apache Polaris REST + S3 (Garage) | Postgres | Parquet in temp bucket |
| `local` | `bronze.duckdb` (same file dbt reads) | `export.duckdb` | — |

The local DuckDB files are the **same files the dbt `local` target uses** (`DBT_DUCKDB_*`), so
`dlt ingest → dbt build → dlt export` works without any external services.

## How to use it

```bash
cd workflows/dlt
uv sync --extra dev

# Full local chain — no external infra needed
export DBT_DUCKDB_DIR=/tmp/duckdb
uv run python -m dlt_runner --pipeline ingest  --project example_db --target local
#   then: cd ../dbt && uv run python -m dbt_runner --project example_db --target local
uv run python -m dlt_runner --pipeline export  --project example_db --target local

# Homelab target — Polaris / S3 / Postgres required
uv run python -m dlt_runner --pipeline ingest  --project example_db --target homelab
uv run python -m dlt_runner --pipeline export  --project example_db --target homelab
```

## Tests

```bash
cd workflows/dlt
uv sync --extra dev

uv run pytest                           # config unit tests + local DuckDB integration
uv run pytest tests/example_db/ -v     # integration only
uv run ruff check .
```

## Docker

```bash
cd workflows/dlt
docker build -t datahub-local-workflows-dlt .
docker run --rm -e DBT_DUCKDB_DIR=/data datahub-local-workflows-dlt \
  --pipeline ingest --project example_db --target local
```

## Environment variables

| Variable | Default | Used by |
|---|---|---|
| `EXAMPLE_DB_SOURCE_URL` | automotive CSV on GitHub | ingest |
| `DLT_TEMP_BUCKET` | `datahub-local-temp` | ingest (homelab) |
| `ICEBERG_CATALOG_URI` | `http://datahub-local-core-data-polaris:8181/api/catalog` | ingest (homelab) |
| `S3_ENDPOINT` / `S3_ACCESS_KEY` / `S3_SECRET_KEY` | Garage endpoint / — | ingest (homelab) |
| `TRINO_HOST` / `TRINO_PORT` / `TRINO_USER` | `datahub-local-core-data-trino` / `8080` / `dbt` | export (homelab) |
| `EXAMPLE_DB_URL` / `EXAMPLE_DB_USER` / `EXAMPLE_DB_PASSWORD` | — | export (homelab) |
| `EXAMPLE_DB_SCHEMA` | `public` | export |
| `DBT_DUCKDB_DIR` / `DBT_DUCKDB_*` | `/tmp/duckdb` | local |
| `DLT_EXPORT_DUCKDB_PATH` | `<dir>/export.duckdb` | export (local) |
