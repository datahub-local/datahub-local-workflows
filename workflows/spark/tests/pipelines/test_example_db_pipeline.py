from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml
from pyspark.sql import SparkSession

from core.launchers import spark_pipelines

PROJECT_ROOT = Path(__file__).resolve().parents[2]

EXAMPLE_DB_ROOT = PROJECT_ROOT / "src" / "pipelines" / "example-db"
EXAMPLE_DB_SPEC_PATH = EXAMPLE_DB_ROOT / "spark-pipeline.yml.j2"
EXAMPLE_DB_SQL_DIR = EXAMPLE_DB_ROOT / "sql"


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("datahub-local-workflows-example-db-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def extract_select_query(rendered_sql: str) -> str:
    _, separator, query = rendered_sql.partition("\nAS\n")
    if not separator:
        raise ValueError("Rendered SQL did not contain a CTAS query body.")
    return query.rsplit(";", 1)[0].strip()


def test_example_db_pipeline_renders_h2_tables_and_env_overrides(
    monkeypatch,
) -> None:
    monkeypatch.setenv("EXAMPLE_DB_SOURCE_URL", "https://example.test/automotive.csv")
    monkeypatch.setenv("EXAMPLE_DB_URL", "jdbc:h2:mem:exampledb")
    monkeypatch.setenv("EXAMPLE_DB_SCHEMA", "analytics")
    monkeypatch.setenv("EXAMPLE_DB_USER", "etl_user")
    monkeypatch.setenv("EXAMPLE_DB_PASSWORD", "topsecret")

    with spark_pipelines.materialize_spec(
        EXAMPLE_DB_SPEC_PATH, "/tmp/example-db-storage"
    ) as rendered_spec_path:
        rendered_spec = yaml.safe_load(rendered_spec_path.read_text())
        staged_root = rendered_spec_path.parent
        source_sql = (staged_root / "sql" / "10_automotive_source.sql").read_text()
        raw_sql = (staged_root / "sql" / "20_automotive_raw.sql").read_text()
        make_sql = (
            staged_root / "sql" / "30_automotive_make_price_summary.sql"
        ).read_text()

        assert rendered_spec["name"] == "datahub-local-workflows-example-db"
        assert rendered_spec["libraries"][0]["glob"]["include"] == "sql/**"
        assert (
            rendered_spec["storage"]
            == Path("/tmp/example-db-storage").resolve().as_uri()
        )
        assert (staged_root / "sql" / "40_automotive_fuel_body_mpg_summary.sql").exists()
        assert "path 'https://example.test/automotive.csv'" in source_sql
        assert "url 'jdbc:h2:mem:exampledb'" in raw_sql
        assert "dbtable 'analytics.automotive_raw'" in raw_sql
        assert "user 'etl_user'" in raw_sql
        assert "password 'topsecret'" in raw_sql
        assert "MAX(updated_date) AS updated_date" in make_sql
        assert "CURRENT_TIMESTAMP() AS created_date" in raw_sql
        assert "CURRENT_TIMESTAMP() AS created_date" in make_sql

    assert not rendered_spec_path.exists()


def test_example_db_aggregation_queries_use_recent_rows_only(
    spark: SparkSession,
) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    rows = [
        {
            "make": "audi",
            "fuel_type": "gas",
            "body_style": "sedan",
            "horsepower": 100,
            "city_mpg": 20,
            "highway_mpg": 30,
            "price": 10000.0,
            "updated_date": now - timedelta(minutes=10),
        },
        {
            "make": "audi",
            "fuel_type": "gas",
            "body_style": "sedan",
            "horsepower": 120,
            "city_mpg": 22,
            "highway_mpg": 32,
            "price": 14000.0,
            "updated_date": now - timedelta(minutes=5),
        },
        {
            "make": "bmw",
            "fuel_type": "diesel",
            "body_style": "wagon",
            "horsepower": 90,
            "city_mpg": 31,
            "highway_mpg": 41,
            "price": 20000.0,
            "updated_date": now - timedelta(hours=2),
        },
        {
            "make": "bmw",
            "fuel_type": "diesel",
            "body_style": "wagon",
            "horsepower": 110,
            "city_mpg": 28,
            "highway_mpg": 38,
            "price": 22000.0,
            "updated_date": now - timedelta(minutes=20),
        },
    ]
    spark.createDataFrame(rows).createOrReplaceTempView("automotive_snapshot")

    with spark_pipelines.materialize_spec(
        EXAMPLE_DB_SPEC_PATH, "/tmp/example-db-storage"
    ) as rendered_spec_path:
        rendered_spec = yaml.safe_load(rendered_spec_path.read_text())
        staged_root = rendered_spec_path.parent
        make_sql = (
            staged_root / "sql" / "30_automotive_make_price_summary.sql"
        ).read_text()
        fuel_sql = (
            staged_root / "sql" / "40_automotive_fuel_body_mpg_summary.sql"
        ).read_text()

        spark.sql(
            "CREATE OR REPLACE TEMP VIEW automotive_make_price_summary_preview AS "
            + extract_select_query(make_sql)
        )
        spark.sql(
            "CREATE OR REPLACE TEMP VIEW automotive_fuel_body_mpg_summary_preview AS "
            + extract_select_query(fuel_sql)
        )

        make_rows = {
            row["make"]: row.asDict()
            for row in spark.table("automotive_make_price_summary_preview").collect()
        }
        fuel_rows = {
            (row["fuel_type"], row["body_style"]): row.asDict()
            for row in spark.table("automotive_fuel_body_mpg_summary_preview").collect()
        }

    assert not rendered_spec_path.exists()

    assert set(make_rows) == {"audi", "bmw"}
    assert make_rows["audi"]["vehicle_count"] == 2
    assert float(make_rows["audi"]["avg_price"]) == pytest.approx(12000.0)
    assert float(make_rows["audi"]["max_price"]) == pytest.approx(14000.0)
    assert float(make_rows["audi"]["avg_horsepower"]) == pytest.approx(110.0)
    assert make_rows["audi"]["created_date"] is not None
    assert make_rows["bmw"]["vehicle_count"] == 2
    assert float(make_rows["bmw"]["avg_price"]) == pytest.approx(21000.0)
    assert make_rows["bmw"]["created_date"] is not None

    assert set(fuel_rows) == {("gas", "sedan"), ("diesel", "wagon")}
    assert fuel_rows[("gas", "sedan")]["vehicle_count"] == 2
    assert float(fuel_rows[("gas", "sedan")]["avg_city_mpg"]) == pytest.approx(21.0)
    assert float(fuel_rows[("gas", "sedan")]["avg_highway_mpg"]) == pytest.approx(31.0)
    assert float(fuel_rows[("gas", "sedan")]["avg_price"]) == pytest.approx(12000.0)
    assert fuel_rows[("gas", "sedan")]["created_date"] is not None
    assert fuel_rows[("diesel", "wagon")]["vehicle_count"] == 2
    assert float(fuel_rows[("diesel", "wagon")]["avg_city_mpg"]) == pytest.approx(29.5)
    assert float(fuel_rows[("diesel", "wagon")]["avg_highway_mpg"]) == pytest.approx(39.5)
    assert float(fuel_rows[("diesel", "wagon")]["avg_price"]) == pytest.approx(21000.0)
    assert fuel_rows[("diesel", "wagon")]["created_date"] is not None