"""Validate example_db dbt project + profile structure without a warehouse connection."""
from pathlib import Path

import yaml

PROJECT_DIR = Path(__file__).parent.parent.parent / "projects" / "example_db"


def _load(name: str) -> dict:
    return yaml.safe_load((PROJECT_DIR / name).read_text())


class TestExampleDbProfiles:
    def setup_method(self):
        self.profile = _load("profiles.yml")["example_db"]

    def test_default_target_is_homelab(self):
        assert self.profile["target"] == "homelab"

    def test_homelab_and_local_targets_defined(self):
        assert set(self.profile["outputs"]) == {"homelab", "local"}

    def test_homelab_uses_trino_local_uses_duckdb(self):
        assert self.profile["outputs"]["homelab"]["type"] == "trino"
        assert self.profile["outputs"]["local"]["type"] == "duckdb"


class TestExampleDbProject:
    def setup_method(self):
        self.project = _load("dbt_project.yml")
        self.models = self.project["models"]["example_db"]

    def test_models_materialized_as_tables(self):
        assert self.models["+materialized"] == "table"

    def test_medallion_layers_target_their_catalogs(self):
        assert self.models["+schema"] == "example_db"
        assert self.models["bronze"]["+database"] == "bronze"
        assert self.models["silver"]["+database"] == "silver"
        assert self.models["gold"]["+database"] == "gold"

    def test_generate_schema_name_macro_present(self):
        macro = PROJECT_DIR / "macros" / "generate_schema_name.sql"
        assert macro.exists()
        assert "custom_schema_name" in macro.read_text()


class TestExampleDbSources:
    def setup_method(self):
        self.sources = _load("models/sources.yml")["sources"]

    def test_automotive_source_in_bronze_catalog(self):
        source = next(s for s in self.sources if s["name"] == "example_db")
        assert source["database"] == "bronze"
        assert source["schema"] == "example_db"
        assert any(t["name"] == "automotive_source" for t in source["tables"])
