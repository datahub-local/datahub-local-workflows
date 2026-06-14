"""Unit tests for the dlt_runner config helpers — no pipeline run required."""
import os

from example_db import config


class TestCatalogMapping:
    def test_medallion_catalogs_map_to_nessie_warehouses(self):
        assert config.CATALOG_WAREHOUSES["bronze"] == "datahub-local-bronze"
        assert config.CATALOG_WAREHOUSES["silver"] == "datahub-local-silver"
        assert config.CATALOG_WAREHOUSES["gold"] == "datahub-local-gold"
        assert config.CATALOG_WAREHOUSES["test"] == "datahub-local-test"

    def test_exports_cover_silver_and_gold_tables(self):
        assert config.EXPORTS[("silver", "example_db", "automotive_raw")] == "automotive_raw"
        assert config.EXPORTS[("gold", "example_db", "automotive_make_price_summary")] == \
            "automotive_make_price_summary"
        assert config.EXPORTS[("gold", "example_db", "automotive_fuel_body_mpg_summary")] == \
            "automotive_fuel_body_mpg_summary"


class TestDuckdbPaths:
    def test_duckdb_path_uses_dbt_env_vars(self, monkeypatch):
        monkeypatch.setenv("DBT_DUCKDB_PATH", "/data/bronze.duckdb")
        assert config.duckdb_path("bronze") == "/data/bronze.duckdb"

    def test_duckdb_path_default_matches_catalog_name(self, monkeypatch):
        monkeypatch.delenv("DBT_DUCKDB_GOLD_PATH", raising=False)
        monkeypatch.setenv("DBT_DUCKDB_DIR", "/wh")
        assert config.duckdb_path("gold") == "/wh/gold.duckdb"


class TestTempBucket:
    def test_default_temp_bucket(self, monkeypatch):
        monkeypatch.delenv("DLT_TEMP_BUCKET", raising=False)
        assert config.temp_bucket() == "datahub-local-temp"


class TestPostgresDsn:
    def test_builds_dsn_from_jdbc_url(self, monkeypatch):
        monkeypatch.delenv("EXAMPLE_DB_DSN", raising=False)
        monkeypatch.setenv("EXAMPLE_DB_URL", "jdbc:postgresql://pg-host:5432/dbt")
        monkeypatch.setenv("EXAMPLE_DB_USER", "pguser")
        monkeypatch.setenv("EXAMPLE_DB_PASSWORD", "secret")
        assert config.postgres_dsn() == "postgresql://pguser:secret@pg-host:5432/dbt"

    def test_explicit_dsn_wins(self, monkeypatch):
        monkeypatch.setenv("EXAMPLE_DB_DSN", "postgresql://u:p@h/db")
        assert config.postgres_dsn() == "postgresql://u:p@h/db"


class TestIcebergEnv:
    def test_configure_iceberg_env_sets_rest_catalog(self, monkeypatch):
        for key in list(os.environ):
            if key.startswith("PYICEBERG_CATALOG__BRONZE__"):
                monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://nessie/iceberg/")
        config.configure_iceberg_env("bronze")
        assert os.environ["PYICEBERG_CATALOG__BRONZE__TYPE"] == "rest"
        assert os.environ["PYICEBERG_CATALOG__BRONZE__URI"] == "http://nessie/iceberg/"
        assert os.environ["PYICEBERG_CATALOG__BRONZE__WAREHOUSE"] == "datahub-local-bronze"


class TestTrinoUrl:
    def test_trino_url_defaults(self, monkeypatch):
        for key in ("TRINO_HOST", "TRINO_PORT", "TRINO_USER"):
            monkeypatch.delenv(key, raising=False)
        assert config.trino_url() == "trino://dbt@datahub-local-core-data-trino:8080"
