from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from core.launchers.spec_utils import materialize_spec

PROJECT_ROOT = Path(__file__).resolve().parents[1]

PIPELINE_ROOT = PROJECT_ROOT / "src" / "pipelines" / "pi" / "sql"
SPEC_PATH = PROJECT_ROOT / "src" / "pipelines" / "pi" / "spark-pipeline.yml.j2"

CONFIG_VALUES = {
    "${spark.pipeline.partitions}": "4",
    "${spark.pipeline.samples_per_partition}": "5",
    "${spark.pipeline.random_seed}": "7",
}


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("datahub-local-workflows-spark-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def render_sql(path: Path) -> str:
    statement = path.read_text()
    for placeholder, value in CONFIG_VALUES.items():
        statement = statement.replace(placeholder, value)
    return statement


def to_local_test_statement(path: Path) -> str:
    statement = render_sql(path)
    if path.name.endswith("_estimate.sql"):
        statement = statement.replace(
            "CREATE MATERIALIZED VIEW pi_estimate AS",
            "CREATE OR REPLACE TEMP VIEW pi_estimate AS",
            1,
        )
    return statement


def test_pipeline_sql_produces_pi_estimate(spark: SparkSession) -> None:
    spark.sql(to_local_test_statement(PIPELINE_ROOT / "10_pi_samples.sql"))
    spark.sql(to_local_test_statement(PIPELINE_ROOT / "20_pi_estimate.sql"))

    row = spark.table("pi_estimate").collect()[0]

    assert row["total_samples"] == 20
    assert row["hit_count"] <= row["total_samples"]
    assert row["pi_estimate"] == pytest.approx(3.0, abs=0.5)
    assert row["partitions"] == 4
    assert row["samples_per_partition"] == 5
    assert row["random_seed"] == 7


def test_pipeline_dry_run_succeeds(tmp_path: Path) -> None:
    project_dir = tmp_path / "spark-pipeline-project"
    (project_dir / "src").mkdir(parents=True)
    shutil.copytree(
        PROJECT_ROOT / "src" / "pipelines",
        project_dir / "src" / "pipelines",
    )
    copied_spec_path = project_dir / "src" / "pipelines" / "pi" / "spark-pipeline.yml.j2"

    command = shutil.which("spark-pipelines")
    if command is None:
        pytest.skip("spark-pipelines CLI is not available in the test environment")

    with materialize_spec(copied_spec_path, str(project_dir / "storage")) as rendered_spec_path:
        result = subprocess.run(
            [
                command,
                "dry-run",
                "--spec",
                str(rendered_spec_path),
            ],
            cwd=project_dir,
            capture_output=True,
            text=True,
            check=False,
        )

    assert result.returncode == 0, result.stderr or result.stdout
