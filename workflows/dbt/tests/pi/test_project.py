"""Validate pi dbt project + profile structure without a warehouse connection."""
from pathlib import Path

import yaml

PROJECT_DIR = Path(__file__).parent.parent.parent / "projects" / "pi"


def _load(name: str) -> dict:
    return yaml.safe_load((PROJECT_DIR / name).read_text())


class TestPiProject:
    def setup_method(self):
        self.project = _load("dbt_project.yml")

    def test_models_materialized_as_tables(self):
        models = self.project["models"]["pi"]
        assert models["+materialized"] == "table"

    def test_monte_carlo_vars_defined(self):
        variables = self.project["vars"]
        assert "partitions" in variables
        assert "samples_per_partition" in variables
        assert "random_seed" in variables


class TestPiProfiles:
    def setup_method(self):
        self.profile = _load("profiles.yml")["pi"]

    def test_default_target_is_homelab(self):
        assert self.profile["target"] == "homelab"

    def test_homelab_uses_trino_local_uses_duckdb(self):
        assert set(self.profile["outputs"]) == {"homelab", "local"}
        assert self.profile["outputs"]["homelab"]["type"] == "trino"
        assert self.profile["outputs"]["local"]["type"] == "duckdb"


def test_models_reference_vars_and_ref():
    samples = (PROJECT_DIR / "models" / "pi_samples.sql").read_text()
    estimate = (PROJECT_DIR / "models" / "pi_estimate.sql").read_text()
    assert "var('partitions')" in samples
    assert "var('samples_per_partition')" in samples
    assert "var('random_seed')" in samples
    assert "ref('pi_samples')" in estimate
