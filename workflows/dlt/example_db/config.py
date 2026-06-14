"""Shared, env-driven configuration for the dlt ingest/export pipelines.

The two targets mirror the dbt project:
- ``homelab`` — Iceberg tables over Nessie REST + S3 (Garage); Postgres for the export.
- ``local``   — DuckDB files (shared with the dbt local target) for both ends.

Each medallion layer is a catalog backed by a Nessie warehouse. DuckDB file paths reuse
the dbt ``DBT_DUCKDB_*`` env vars so a local ingest lands in the same files dbt reads.
"""

from __future__ import annotations

import os

# Medallion catalog -> Nessie warehouse (matches the dbt Trino catalogs).
CATALOG_WAREHOUSES = {
    "bronze": "datahub-local-bronze",
    "silver": "datahub-local-silver",
    "gold": "datahub-local-gold",
    "test": "datahub-local-test",
}

# dbt-built tables exported to Postgres: (catalog, schema, table) -> target table name.
EXPORTS = {
    ("silver", "example_db", "automotive_raw"): "automotive_raw",
    ("gold", "example_db", "automotive_make_price_summary"): "automotive_make_price_summary",
    ("gold", "example_db", "automotive_fuel_body_mpg_summary"): "automotive_fuel_body_mpg_summary",
}

SOURCE_DEFAULT_URL = (
    "https://raw.githubusercontent.com/Opensourcefordatascience/"
    "Data-sets/refs/heads/master/automotive_data.csv"
)

# DuckDB file per catalog for the local target (same env vars the dbt local profile uses).
_DUCKDB_ENV = {
    "bronze": "DBT_DUCKDB_PATH",
    "silver": "DBT_DUCKDB_SILVER_PATH",
    "gold": "DBT_DUCKDB_GOLD_PATH",
    "test": "DBT_DUCKDB_PI_PATH",
}


def env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    return value if value not in (None, "") else default


def source_url() -> str:
    return env("EXAMPLE_DB_SOURCE_URL", SOURCE_DEFAULT_URL)


def temp_bucket() -> str:
    """S3 bucket dlt stages temporal (parquet) datasets in before the Iceberg commit."""
    return env("DLT_TEMP_BUCKET", "datahub-local-temp")


def duckdb_path(catalog: str) -> str:
    default_dir = env("DBT_DUCKDB_DIR", "/tmp/duckdb")
    return env(_DUCKDB_ENV[catalog], f"{default_dir}/{catalog}.duckdb")


def export_duckdb_path() -> str:
    """Local stand-in for the Postgres export target."""
    default_dir = env("DBT_DUCKDB_DIR", "/tmp/duckdb")
    return env("DLT_EXPORT_DUCKDB_PATH", f"{default_dir}/export.duckdb")


def export_schema() -> str:
    return env("EXAMPLE_DB_SCHEMA", "public")


def nessie_uri() -> str:
    return env("ICEBERG_CATALOG_URI", "http://datahub-local-core-data-nessie:19120/iceberg/")


def s3_endpoint() -> str:
    return env("S3_ENDPOINT", "http://datahub-local-core-data-garage:3900")


def configure_iceberg_env(catalog: str) -> None:
    """Wire pyiceberg's REST catalog + S3 config (read by dlt's filesystem-iceberg path).

    pyiceberg resolves a catalog named ``catalog`` from ``PYICEBERG_CATALOG__<name>__*``
    env vars (see https://py.iceberg.apache.org/configuration/). The warehouse is the
    Nessie warehouse for this medallion layer.
    """
    prefix = f"PYICEBERG_CATALOG__{catalog.upper()}__"
    defaults = {
        f"{prefix}TYPE": "rest",
        f"{prefix}URI": nessie_uri(),
        f"{prefix}WAREHOUSE": CATALOG_WAREHOUSES[catalog],
        f"{prefix}S3__ENDPOINT": s3_endpoint(),
        f"{prefix}S3__ACCESS_KEY_ID": env("S3_ACCESS_KEY", ""),
        f"{prefix}S3__SECRET_ACCESS_KEY": env("S3_SECRET_KEY", ""),
        f"{prefix}S3__PATH_STYLE_ACCESS": "true",
    }
    for key, value in defaults.items():
        os.environ.setdefault(key, value)


def postgres_dsn() -> str:
    """Build a SQLAlchemy/dlt Postgres DSN from the EXAMPLE_DB_* secrets.

    Accepts a ready DSN in ``EXAMPLE_DB_DSN`` or derives one from a JDBC ``EXAMPLE_DB_URL``
    (``jdbc:postgresql://host:port/db``) plus user/password.
    """
    dsn = env("EXAMPLE_DB_DSN")
    if dsn:
        return dsn

    url = os.environ["EXAMPLE_DB_URL"]
    user = os.environ["EXAMPLE_DB_USER"]
    password = env("EXAMPLE_DB_PASSWORD", "")
    host_db = url.split("//", 1)[1] if "//" in url else url
    return f"postgresql://{user}:{password}@{host_db}"


def trino_url() -> str:
    host = env("TRINO_HOST", "datahub-local-core-data-trino")
    port = env("TRINO_PORT", "8080")
    user = env("TRINO_USER", "dbt")
    return f"trino://{user}@{host}:{port}"
