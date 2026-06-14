"""Shared, env-driven configuration for all dlt pipelines.

The two targets mirror the dbt project:
- ``homelab`` — Iceberg tables over Apache Polaris REST + S3 (Garage); Postgres for export.
- ``local``   — DuckDB files (shared with the dbt local target).
"""

from __future__ import annotations

import os

# DuckDB file per catalog for the local target (same env vars the dbt local profile uses).
_DUCKDB_ENV = {
    "bronze": "DBT_DUCKDB_PATH",
    "silver": "DBT_DUCKDB_SILVER_PATH",
    "gold": "DBT_DUCKDB_GOLD_PATH",
    "test": "DBT_DUCKDB_PI_PATH",
}

VALID_TARGETS = ("homelab", "local")


def validate_target(target: str) -> None:
    if target not in VALID_TARGETS:
        raise ValueError(f"unknown target: {target!r}")


def env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    return value if value not in (None, "") else default


def temp_bucket() -> str:
    """S3 bucket dlt stages temporal (parquet) datasets in before the Iceberg commit."""
    return env("DLT_TEMP_BUCKET", "datahub-local-temp")


def duckdb_path(catalog: str) -> str:
    default_dir = env("DBT_DUCKDB_DIR", "/tmp/duckdb")
    return env(_DUCKDB_ENV[catalog], f"{default_dir}/{catalog}.duckdb")


def polaris_uri() -> str:
    return env("ICEBERG_CATALOG_URI", "http://datahub-local-core-data-polaris:8181/api/catalog")


def s3_endpoint() -> str:
    return env("S3_ENDPOINT", "http://datahub-local-core-data-garage:3900")


def configure_iceberg_env(catalog: str, warehouse: str | None = None) -> None:
    """Wire pyiceberg's REST catalog + S3 config (read by dlt's filesystem-iceberg path).

    pyiceberg resolves a catalog named ``catalog`` from ``PYICEBERG_CATALOG__<name>__*``
    env vars (see https://py.iceberg.apache.org/configuration/). ``warehouse`` is the
    Apache Polaris catalog name; defaults to ``catalog`` when they share the same name.

    Auth (required by Apache Polaris): set ``POLARIS_CREDENTIAL`` (``client_id:client_secret``
    for OAuth2 client-credentials flow) or ``POLARIS_TOKEN`` (pre-fetched bearer token).
    """
    if warehouse is None:
        warehouse = catalog
    prefix = f"PYICEBERG_CATALOG__{catalog.upper()}__"
    defaults = {
        f"{prefix}TYPE": "rest",
        f"{prefix}URI": polaris_uri(),
        f"{prefix}WAREHOUSE": warehouse,
        f"{prefix}S3__ENDPOINT": s3_endpoint(),
        f"{prefix}S3__ACCESS_KEY_ID": env("S3_ACCESS_KEY", ""),
        f"{prefix}S3__SECRET_ACCESS_KEY": env("S3_SECRET_KEY", ""),
        f"{prefix}S3__PATH_STYLE_ACCESS": "true",
    }
    credential = env("POLARIS_CREDENTIAL")
    if credential:
        defaults[f"{prefix}CREDENTIAL"] = credential
    elif token := env("POLARIS_TOKEN"):
        defaults[f"{prefix}TOKEN"] = token
    for key, value in defaults.items():
        os.environ.setdefault(key, value)


def trino_url() -> str:
    host = env("TRINO_HOST", "datahub-local-core-data-trino")
    port = env("TRINO_PORT", "8080")
    user = env("TRINO_USER", "dbt")
    return f"trino://{user}@{host}:{port}"
