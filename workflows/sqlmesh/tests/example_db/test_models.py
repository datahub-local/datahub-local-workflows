"""Validate model metadata using AST parsing — no Spark session required."""
import ast
from pathlib import Path

MODELS_DIR = Path(__file__).parent.parent.parent / "pipelines" / "example_db" / "models"


def _parse_model_decorator(path: Path) -> dict:
    """Return args from the @model(...) decorator in a Python model file.

    The first positional arg is the model name, stored under key "name".
    All keyword args are stored by their keyword.
    """
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call):
                continue
            func = decorator.func
            fname = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
            if fname != "model":
                continue
            result = {}
            if decorator.args:
                result["name"] = ast.literal_eval(decorator.args[0])
            for kw in decorator.keywords:
                if kw.arg is not None:
                    result[kw.arg] = ast.literal_eval(kw.value)
            return result
    return {}


class TestAutomotiveSourceModel:
    def setup_method(self):
        self.meta = _parse_model_decorator(
            MODELS_DIR / "bronze" / "automotive_source.py"
        )

    def test_model_name(self):
        assert self.meta.get("name") == "example_db.automotive_source"

    def test_no_explicit_catalog(self):
        assert "catalog" not in self.meta

    def test_kind_is_full(self):
        assert self.meta.get("kind") == "FULL"

    def test_dialect_is_spark(self):
        assert self.meta.get("dialect") == "spark"

    def test_columns_are_all_strings(self):
        for col, dtype in self.meta.get("columns", {}).items():
            assert dtype == "STRING", f"Column {col} expected STRING, got {dtype}"


class TestAutomotiveRawModel:
    def setup_method(self):
        self.meta = _parse_model_decorator(
            MODELS_DIR / "silver" / "automotive_raw.py"
        )

    def test_model_name_includes_silver_catalog(self):
        assert self.meta.get("name") == "silver.example_db.automotive_raw"

    def test_kind_is_full(self):
        assert self.meta.get("kind") == "FULL"

    def test_depends_on_snapshot(self):
        assert "example_db.automotive_snapshot" in self.meta.get("depends_on", [])

    def test_has_updated_and_created_date(self):
        cols = self.meta.get("columns", {})
        assert "updated_date" in cols
        assert "created_date" in cols


class TestAutomotiveMakePriceSummaryModel:
    def setup_method(self):
        self.meta = _parse_model_decorator(
            MODELS_DIR / "gold" / "automotive_make_price_summary.py"
        )

    def test_model_name_includes_gold_catalog(self):
        assert self.meta.get("name") == "gold.example_db.automotive_make_price_summary"

    def test_depends_on_snapshot(self):
        assert "example_db.automotive_snapshot" in self.meta.get("depends_on", [])

    def test_expected_columns(self):
        cols = self.meta.get("columns", {})
        for expected in ("make", "vehicle_count", "avg_price", "max_price", "avg_horsepower"):
            assert expected in cols, f"Missing column: {expected}"


class TestAutomotiveFuelBodyMpgSummaryModel:
    def setup_method(self):
        self.meta = _parse_model_decorator(
            MODELS_DIR / "gold" / "automotive_fuel_body_mpg_summary.py"
        )

    def test_model_name_includes_gold_catalog(self):
        assert self.meta.get("name") == "gold.example_db.automotive_fuel_body_mpg_summary"

    def test_depends_on_snapshot(self):
        assert "example_db.automotive_snapshot" in self.meta.get("depends_on", [])

    def test_expected_columns(self):
        cols = self.meta.get("columns", {})
        for expected in ("fuel_type", "body_style", "vehicle_count", "avg_city_mpg", "avg_highway_mpg"):
            assert expected in cols, f"Missing column: {expected}"


class TestAutomotiveSnapshotSql:
    def setup_method(self):
        self.sql = (MODELS_DIR / "bronze" / "automotive_snapshot.sql").read_text()

    def test_model_name(self):
        assert "example_db.automotive_snapshot" in self.sql

    def test_kind_is_full(self):
        assert "FULL" in self.sql

    def test_filters_empty_make(self):
        assert "make" in self.sql.lower()
        assert "WHERE" in self.sql or "where" in self.sql

    def test_expected_output_columns(self):
        expected = [
            "symboling", "normalized_losses", "make", "fuel_type",
            "city_mpg", "highway_mpg", "price", "updated_date",
        ]
        sql_lower = self.sql.lower()
        for col in expected:
            assert col in sql_lower, f"Expected column alias '{col}' not found in snapshot SQL"

    def test_casts_numeric_fields(self):
        assert "CAST" in self.sql or "cast" in self.sql

    def test_replaces_question_mark_with_null(self):
        assert "NULLIF" in self.sql or "nullif" in self.sql
