"""Integration tests: run the example_db pipeline against the local gateway.

These tests start a real PySpark session (local[*]) with local Iceberg
HadoopCatalogs and an in-memory H2 JDBC target.  They are intentionally
slow (~1-3 min on first run when Spark JARs are downloaded) and are kept
separate from the fast unit tests in test_config/test_jdbc/test_models.
"""
import os
from pathlib import Path

import pytest

PIPELINE_DIR = Path(__file__).parent.parent.parent / "pipelines" / "example_db"


@pytest.fixture(scope="module")
def ctx(tmp_path_factory):
    from sqlmesh import Context
    from sqlmesh.core.config import Config, GatewayConfig
    from sqlmesh.core.config.connection import DuckDBConnectionConfig
    from sqlmesh.core.config.loader import load_config_from_paths

    tmp = tmp_path_factory.mktemp("sqlmesh_integration")
    warehouse_dir = tmp / "warehouse"
    warehouse_dir.mkdir()

    os.environ["SQLMESH_LOCAL_GATEWAY"] = "true"
    os.environ["SPARK_WAREHOUSE_DIR"] = str(warehouse_dir)
    os.environ["SQLMESH_LOCAL_STATE_DB"] = str(tmp / "state.db")
    os.environ["EXAMPLE_DB_URL"] = f"jdbc:h2:mem:testdb_{os.getpid()};DB_CLOSE_DELAY=-1"
    os.environ["EXAMPLE_DB_USER"] = "sa"
    os.environ["EXAMPLE_DB_PASSWORD"] = ""
    os.environ["ICEBERG_BRONZE_URI"] = f"jdbc:h2:file:{tmp}/iceberg_bronze;DB_CLOSE_DELAY=-1"
    os.environ["ICEBERG_SILVER_URI"] = f"jdbc:h2:file:{tmp}/iceberg_silver;DB_CLOSE_DELAY=-1"
    os.environ["ICEBERG_GOLD_URI"] = f"jdbc:h2:file:{tmp}/iceberg_gold;DB_CLOSE_DELAY=-1"
    os.environ.setdefault("SQLMESH_STATE_HOST", "localhost")
    os.environ.setdefault("SQLMESH_STATE_USER", "user")
    os.environ.setdefault("S3_ACCESS_KEY", "test")
    os.environ.setdefault("S3_SECRET_KEY", "test")

    # Override homelab with DuckDB so engine_adapters doesn't start a second
    # Spark session with conflicting catalog config.
    file_cfg = load_config_from_paths(Config, project_paths=[PIPELINE_DIR / "config.yaml"])
    merged = file_cfg.model_copy(
        update={
            "gateways": {
                **file_cfg.gateways,
                "homelab": GatewayConfig(
                    connection=DuckDBConnectionConfig(database=":memory:"),
                    state_connection=DuckDBConnectionConfig(database=":memory:"),
                ),
            }
        }
    )

    context = Context(paths=[str(PIPELINE_DIR)], gateway="local", config=merged)
    context.plan(auto_apply=True, no_prompts=True)

    yield context

    context.close()


def test_all_models_loaded(ctx):
    actual = {name.replace('"', '') for name in ctx.models}
    expected = {
        "bronze.example_db.automotive_source",
        "bronze.example_db.automotive_snapshot",
        "silver.example_db.automotive_raw",
        "gold.example_db.automotive_make_price_summary",
        "gold.example_db.automotive_fuel_body_mpg_summary",
    }
    assert expected.issubset(actual)


def test_automotive_source_has_rows(ctx):
    df = ctx.engine_adapter.fetchdf(
        "SELECT COUNT(*) AS cnt FROM example_db.automotive_source"
    )
    assert int(df["cnt"].iloc[0]) > 0


def test_automotive_snapshot_drops_empty_makes(ctx):
    df = ctx.engine_adapter.fetchdf(
        "SELECT COUNT(*) AS cnt FROM example_db.automotive_snapshot "
        "WHERE make IS NULL OR TRIM(make) = ''"
    )
    assert int(df["cnt"].iloc[0]) == 0


def test_automotive_snapshot_casts_numerics(ctx):
    df = ctx.engine_adapter.fetchdf(
        "SELECT MIN(city_mpg) AS mn, MAX(highway_mpg) AS mx "
        "FROM example_db.automotive_snapshot"
    )
    assert int(df["mn"].iloc[0]) > 0
    assert int(df["mx"].iloc[0]) > int(df["mn"].iloc[0])


def test_automotive_raw_in_silver_catalog(ctx):
    df = ctx.engine_adapter.fetchdf(
        "SELECT COUNT(*) AS cnt FROM silver.example_db.automotive_raw"
    )
    assert int(df["cnt"].iloc[0]) > 0


def test_make_price_summary_aggregated(ctx):
    df = ctx.engine_adapter.fetchdf(
        "SELECT COUNT(DISTINCT make) AS makes, MIN(avg_price) AS mn "
        "FROM gold.example_db.automotive_make_price_summary"
    )
    assert int(df["makes"].iloc[0]) > 1
    assert float(df["mn"].iloc[0]) > 0


def test_fuel_body_mpg_summary_in_gold_catalog(ctx):
    df = ctx.engine_adapter.fetchdf(
        "SELECT COUNT(*) AS cnt FROM gold.example_db.automotive_fuel_body_mpg_summary"
    )
    assert int(df["cnt"].iloc[0]) > 0
