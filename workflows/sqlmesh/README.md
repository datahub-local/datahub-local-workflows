
# SQLMesh Project

This folder contains SQLMesh pipelines for local development and CI.

## Contents

- `pyproject.toml` — canonical dependency manifest (used by `uv`)
- `.python-version` — pinned Python version for `uv`
- `Dockerfile` — image used to run SQLMesh pipelines in containers
- `example_db/` — automotive dataset pipeline (Spark/Nessie/Iceberg + DuckDB local)
- `pi/` — Monte Carlo π estimation pipeline (Spark/Nessie/Iceberg + DuckDB local)

## How to use it

This project uses `uv` as the primary dependency manager. `pyproject.toml` is
the canonical source of dependencies.

- Install `uv` (one-time per machine):

    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    # or: pip install uv
    ```

- Install project dependencies:

    ```bash
    cd workflows/sqlmesh
    uv pip install -e .
    ```

- Run SQLMesh commands (from the project directory):

    ```bash
    # plan against the local DuckDB gateway
    uv run sqlmesh -p example_db --gateway local plan

    # apply changes
    uv run sqlmesh -p example_db --gateway local run

    # use the default Spark/Nessie gateway
    uv run sqlmesh -p example_db plan
    uv run sqlmesh -p pi run
    ```

## Gateways

Each project supports three gateways defined in `config.yaml`:

| Gateway | Engine | State store | Use case |
|---------|--------|-------------|----------|
| `spark` (default) | PySpark + Nessie/Iceberg | PostgreSQL | Production / CI |
| `local` | Spark (built-in catalog) | DuckDB | Local dev, no external catalog needed |

## Environment variables

### Spark / Nessie gateway

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `NESSIE_URI` | `http://nessie:19120/api/v2` | Nessie catalog REST endpoint |
| `NESSIE_REF` | `main` | Nessie branch/tag |
| `NESSIE_WAREHOUSE` | — | Warehouse path (e.g. `s3://bucket/warehouse`) |
| `SQLMESH_STATE_HOST` | — | PostgreSQL host for state |
| `SQLMESH_STATE_USER` | — | PostgreSQL user for state |
| `SQLMESH_STATE_PASSWORD` | — | PostgreSQL password for state |

### `pi` pipeline extras

| Variable | Default | Description |
|----------|---------|-------------|
| `PI_PARTITIONS` | `500` | Number of Spark partitions |
| `PI_SAMPLES_PER_PARTITION` | `100000` | Random samples per partition |
| `PI_RANDOM_SEED` | `7` | RNG seed for reproducibility |

## Docker

Build and run the image:

```bash
cd workflows/sqlmesh

docker build -t sqlmesh-pipelines .

# run against local DuckDB (no external infra)
docker run --rm \
  -v $(pwd)/example_db:/app/example_db \
  sqlmesh-pipelines -p example_db --gateway local plan

# run against Spark/Nessie (pass env vars)
docker run --rm \
  -e NESSIE_URI=http://nessie:19120/api/v2 \
  -e NESSIE_WAREHOUSE=s3://my-bucket/warehouse \
  -e SQLMESH_STATE_HOST=postgres \
  -e SQLMESH_STATE_USER=sqlmesh \
  -e SQLMESH_STATE_PASSWORD=secret \
  sqlmesh-pipelines -p pi run
```
