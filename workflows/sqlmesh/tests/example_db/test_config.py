"""Validate example_db pipeline config.yaml without running Spark."""
import os
from pathlib import Path

from sqlmesh.utils.yaml import load as yaml_load

PIPELINES_DIR = Path(__file__).parent.parent.parent / "pipelines"


def _load_config(local_gateway: bool = False) -> dict:
    path = PIPELINES_DIR / "example_db" / "config.yaml"
    env = {"SQLMESH_LOCAL_GATEWAY": "true" if local_gateway else "false"}
    original = {k: os.environ.get(k) for k in env}
    try:
        os.environ.update(env)
        return yaml_load(path)
    finally:
        for k, v in original.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class TestExampleDbConfig:
    def setup_method(self):
        self.config = _load_config(local_gateway=False)
        self.config_with_local = _load_config(local_gateway=True)

    def test_default_gateway_is_homelab_in_production(self):
        assert self.config["default_gateway"] == "homelab"

    def test_default_gateway_is_local_when_local_gateway_enabled(self):
        assert self.config_with_local["default_gateway"] == "local"

    def test_only_homelab_gateway_defined_in_production(self):
        gateways = self.config["gateways"]
        assert "homelab" in gateways
        assert "local" not in gateways

    def test_homelab_and_local_gateways_defined_when_enabled(self):
        gateways = self.config_with_local["gateways"]
        assert "homelab" in gateways
        assert "local" in gateways

    def test_model_defaults_has_dialect(self):
        assert self.config["model_defaults"]["dialect"] == "spark"

    def test_model_defaults_has_no_catalog(self):
        assert "catalog" not in self.config["model_defaults"]

    def test_homelab_connection_type_is_spark(self):
        assert self.config["gateways"]["homelab"]["connection"]["type"] == "spark"

    def test_local_state_connection_is_duckdb(self):
        sc = self.config_with_local["gateways"]["local"]["state_connection"]
        assert sc["type"] == "duckdb"
        assert sc["database"] is not None

    def test_homelab_state_connection_is_postgres(self):
        assert self.config["gateways"]["homelab"]["state_connection"]["type"] == "postgres"

    def test_spark_default_catalog_is_set(self):
        spark_config = self.config["gateways"]["homelab"]["connection"]["config"]
        assert spark_config.get("spark.sql.defaultCatalog") == "bronze"

    def test_three_iceberg_catalogs_defined(self):
        spark_config = self.config["gateways"]["homelab"]["connection"]["config"]
        catalogs = {k.split(".")[3] for k in spark_config if k.startswith("spark.sql.catalog.")}
        assert catalogs == {"bronze", "silver", "gold"}

    def test_local_gateway_has_three_iceberg_catalogs(self):
        spark_config = self.config_with_local["gateways"]["local"]["connection"]["config"]
        catalogs = {k.split(".")[3] for k in spark_config if k.startswith("spark.sql.catalog.")}
        assert catalogs == {"bronze", "silver", "gold"}

    def test_local_gateway_iceberg_catalogs_use_jdbc(self):
        spark_config = self.config_with_local["gateways"]["local"]["connection"]["config"]
        for cat in ("bronze", "silver", "gold"):
            assert spark_config.get(f"spark.sql.catalog.{cat}.catalog-impl") == \
                "org.apache.iceberg.jdbc.JdbcCatalog"

    def test_local_gateway_default_catalog_is_bronze(self):
        spark_config = self.config_with_local["gateways"]["local"]["connection"]["config"]
        assert spark_config.get("spark.sql.defaultCatalog") == "bronze"
