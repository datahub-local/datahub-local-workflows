"""Validate example_db model SQL and that the project parses (no warehouse required)."""
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent.parent / "example_db"
MODELS_DIR = PROJECT_DIR / "models"


class TestAutomotiveSnapshot:
    def setup_method(self):
        self.sql = (MODELS_DIR / "bronze" / "automotive_snapshot.sql").read_text()

    def test_reads_from_source(self):
        assert "source('example_db', 'automotive_source')" in self.sql

    def test_filters_empty_make(self):
        assert "where" in self.sql.lower()
        assert "make" in self.sql.lower()

    def test_casts_and_nullifs(self):
        assert "CAST" in self.sql
        assert "NULLIF" in self.sql

    def test_expected_output_columns(self):
        lower = self.sql.lower()
        for col in ("symboling", "normalized_losses", "make", "fuel_type",
                    "city_mpg", "highway_mpg", "price", "updated_date"):
            assert col in lower, f"missing column alias {col}"


class TestDownstreamModels:
    def test_silver_raw_refs_snapshot_and_adds_created_date(self):
        sql = (MODELS_DIR / "silver" / "automotive_raw.sql").read_text()
        assert "ref('automotive_snapshot')" in sql
        assert "created_date" in sql.lower()

    def test_make_price_summary_columns(self):
        sql = (MODELS_DIR / "gold" / "automotive_make_price_summary.sql").read_text().lower()
        assert "ref('automotive_snapshot')" in sql
        for col in ("make", "vehicle_count", "avg_price", "max_price", "avg_horsepower"):
            assert col in sql, f"missing column {col}"

    def test_fuel_body_mpg_summary_columns(self):
        sql = (MODELS_DIR / "gold" / "automotive_fuel_body_mpg_summary.sql").read_text().lower()
        assert "ref('automotive_snapshot')" in sql
        for col in ("fuel_type", "body_style", "vehicle_count", "avg_city_mpg", "avg_highway_mpg"):
            assert col in sql, f"missing column {col}"


def test_project_parses():
    """dbt parse validates refs/sources/macros without a warehouse connection."""
    from dbt.cli.main import dbtRunner

    result = dbtRunner().invoke([
        "parse",
        "--project-dir", str(PROJECT_DIR),
        "--profiles-dir", str(PROJECT_DIR),
        "--target", "local",
    ])
    assert result.success, getattr(result, "exception", "dbt parse failed")
