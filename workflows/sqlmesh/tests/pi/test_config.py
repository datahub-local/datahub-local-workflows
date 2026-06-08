"""Validate pi pipeline config.yaml without running Spark."""
import os
from pathlib import Path

from sqlmesh.utils.yaml import load as yaml_load

PIPELINES_DIR = Path(__file__).parent.parent.parent / "pipelines"


def _load_config(local_gateway: bool = False) -> dict:
    path = PIPELINES_DIR / "pi" / "config.yaml"
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


class TestPiConfig:
    def setup_method(self):
        self.config = _load_config(local_gateway=False)

    def test_default_gateway_is_homelab_in_production(self):
        assert self.config["default_gateway"] == "homelab"

    def test_default_gateway_is_local_when_enabled(self):
        config = _load_config(local_gateway=True)
        assert config["default_gateway"] == "local"

    def test_model_defaults_has_dialect(self):
        assert self.config["model_defaults"]["dialect"] == "spark"

    def test_model_defaults_has_no_catalog(self):
        assert "catalog" not in self.config["model_defaults"]

    def test_variables_defined(self):
        assert "partitions" in self.config["variables"]
        assert "samples_per_partition" in self.config["variables"]
