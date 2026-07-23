# dbt Core Pipelines

dbt Core transformation pipelines for `datahub-local-ai`. Runs on **Trino** (homelab,
Iceberg over Apache Polaris) or **DuckDB** (local). CSV ingestion and reverse-ETL export are handled
by the separate [`../dlt`](../dlt) pipelines, not by dbt.

## Contents

```
workflows/dbt/
  dbt_runner/         entry point: python -m dbt_runner --project <name> --target <env>
  example_db/         automotive dataset pipeline (medallion: bronze/silver/gold catalogs)
  pi/                 Monte Carlo π estimation pipeline
  tests/
    example_db/       integration test (seeds bronze, runs dbt build, asserts tables)
    pi/               structural / parse tests
  Dockerfile
  pyproject.toml      package: datahub-local-ai-dbt
```

## Pipelines

| Pipeline     | Description                                                | Targets            |
| ------------ | ---------------------------------------------------------- | ------------------ |
| `example_db` | Automotive dataset — bronze → silver → gold medallion      | `homelab`, `local` |
| `pi`         | Monte Carlo π estimation (DuckDB/Trino adapter-dispatched) | `homelab`, `local` |

## Targets

| Target              | Engine                                                      | Catalogs              | Use case                      |
| ------------------- | ----------------------------------------------------------- | --------------------- | ----------------------------- |
| `homelab` (default) | Trino on Kubernetes (Iceberg over Apache Polaris REST + S3) | Polaris catalogs      | Production / CI               |
| `local`             | DuckDB (one file per catalog)                               | Attached DuckDB files | Local dev — no external infra |

`database.schema.table` resolves identically on both engines.

## How to use it

```bash
cd workflows/dbt
uv sync --extra dev

# Local target — no external infra needed
uv run python -m dbt_runner --project pi --target local
uv run python -m dbt_runner --project example_db --target local   # seed bronze first (dlt ingest or integration test)

# Homelab target — Trino / Polaris / S3 required
uv run python -m dbt_runner --project example_db --target homelab

# Validate without a warehouse connection
uv run dbt parse --project-dir pi --profiles-dir pi --target local
```

## Tests

```bash
cd workflows/dbt
uv sync --extra dev

# Fast structural / SQL / dbt parse tests — no warehouse
uv run pytest tests/ --ignore=tests/example_db/test_integration.py

# DuckDB end-to-end — seeds bronze, runs dbt build, asserts medallion tables
uv run pytest tests/example_db/test_integration.py -v

uv run ruff check .
```

## Docker

```bash
cd workflows/dbt
docker build -t datahub-local-ai-dbt .
docker run --rm datahub-local-ai-dbt --project pi --target local
```

## Environment variables

### `homelab` target (Trino)

| Variable     | Default                         | Description            |
| ------------ | ------------------------------- | ---------------------- |
| `TRINO_HOST` | `datahub-local-core-data-trino` | Trino coordinator host |
| `TRINO_PORT` | `8080`                          | Trino coordinator port |
| `TRINO_USER` | `dbt`                           | Trino user             |

### `local` target (DuckDB)

| Variable                 | Default                     | Description                      |
| ------------------------ | --------------------------- | -------------------------------- |
| `DBT_DUCKDB_PATH`        | `/tmp/duckdb/bronze.duckdb` | bronze catalog file (example_db) |
| `DBT_DUCKDB_SILVER_PATH` | `/tmp/duckdb/silver.duckdb` | silver catalog file (example_db) |
| `DBT_DUCKDB_GOLD_PATH`   | `/tmp/duckdb/gold.duckdb`   | gold catalog file (example_db)   |
| `DBT_DUCKDB_PI_PATH`     | `/tmp/duckdb/test.duckdb`   | test catalog file (pi)           |

### `pi` pipeline extras

| Variable                   | Default  | Description                                         |
| -------------------------- | -------- | --------------------------------------------------- |
| `PI_PARTITIONS`            | `500`    | Outer grid dimension                                |
| `PI_SAMPLES_PER_PARTITION` | `100000` | Inner grid dimension (× partitions)                 |
| `PI_RANDOM_SEED`           | `7`      | Carried as a column (engine `random()` is unseeded) |

## Medallion catalogs (example_db)

Models write to three catalogs — `bronze`, `silver`, `gold`. With Trino these are real
Iceberg catalogs backed by Apache Polaris; with DuckDB they are separate attached files.

The Trino catalog names (`bronze`, `silver`, `gold`, `test`) map directly to Apache Polaris
catalog names. dbt does not create catalogs — they are provisioned on the Polaris server.
