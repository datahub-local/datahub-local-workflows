from __future__ import annotations

from pathlib import Path
import subprocess
import sys
from textwrap import dedent

import yaml

from core.launchers import spark_pipelines

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def write_spec(path: Path, contents: str) -> None:
    path.write_text(dedent(contents).strip() + "\n")


def materialized_library_root(rendered_spec_path: Path, rendered_spec: dict) -> Path:
    include = rendered_spec["libraries"][0]["glob"]["include"]
    return rendered_spec_path.parent / include.removesuffix("/**")


def test_launcher_script_runs_when_executed_directly() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "src" / "core" / "launchers" / "spark_pipelines.py"),
            "--help",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert "Run the Spark declarative pipeline." in result.stdout


def test_materialize_spec_renders_jinja_template_and_overrides_storage(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "spark-pipeline.yaml.j2"
    write_spec(
        spec_path,
        """
        name: templated-pipeline
        storage: file:///ignored
        configuration:
          spark.test.pipeline_storage: "{{ pipeline_storage }}"
        libraries:
          - glob:
              include: sql/**
        """,
    )

    with spark_pipelines.materialize_spec(
        spec_path, "/tmp/templated-storage"
    ) as rendered_path:
        rendered = yaml.safe_load(rendered_path.read_text())

        assert rendered_path.parent == spec_path.parent
        assert rendered_path != spec_path
        assert rendered["storage"] == Path("/tmp/templated-storage").resolve().as_uri()
        assert (
            rendered["configuration"]["spark.test.pipeline_storage"]
            == "/tmp/templated-storage"
        )

    assert not rendered_path.exists()


def test_materialize_spec_renders_imported_sql_templates(tmp_path: Path) -> None:
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir()

    spec_path = tmp_path / "spark-pipeline.yml"
    write_spec(
        spec_path,
        """
        name: templated-sql-pipeline
        storage: file:///ignored
        libraries:
          - glob:
                            include: sql/**
        """,
    )
    write_spec(
        sql_dir / "10_template.sql.j2",
        """
        SELECT '{{ pipeline_storage }}' AS pipeline_storage
        """,
    )

    rendered_sql_path = sql_dir / "10_template.sql"

    with spark_pipelines.materialize_spec(
        spec_path, "/tmp/rendered-sql-storage"
    ) as rendered_spec_path:
        rendered_spec = yaml.safe_load(rendered_spec_path.read_text())
        library_root = materialized_library_root(rendered_spec_path, rendered_spec)
        staged_sql_path = library_root / "sql" / "10_template.sql"

        assert not rendered_sql_path.exists()
        assert staged_sql_path.exists()
        assert (
            staged_sql_path.read_text().strip()
            == "SELECT '/tmp/rendered-sql-storage' AS pipeline_storage"
        )

    assert not rendered_sql_path.exists()
    assert not library_root.exists()


def test_materialize_spec_renders_sql_templates_with_shared_macros(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("TEST_PIPELINE_NAME", "launcher-test")

    sql_dir = tmp_path / "sql"
    lib_dir = tmp_path / "lib"
    sql_dir.mkdir()
    lib_dir.mkdir()

    spec_path = tmp_path / "spark-pipeline.yml"
    write_spec(
        spec_path,
        """
        name: templated-sql-pipeline
        storage: file:///ignored
        libraries:
          - glob:
              include: sql/**
        """,
    )
    write_spec(
        lib_dir / "shared.sql.j2",
        """
        {% macro project_name(name) -%}
        SELECT '{{ name }}' AS project_name
        {%- endmacro %}
        """,
    )
    write_spec(
        sql_dir / "10_template.sql.j2",
        """
        {% import 'lib/shared.sql.j2' as shared %}
        {{ shared.project_name(env.TEST_PIPELINE_NAME) }}
        """,
    )

    rendered_sql_path = sql_dir / "10_template.sql"

    with spark_pipelines.materialize_spec(
        spec_path, "/tmp/rendered-sql-storage"
    ) as rendered_spec_path:
        rendered_spec = yaml.safe_load(rendered_spec_path.read_text())
        library_root = materialized_library_root(rendered_spec_path, rendered_spec)
        staged_sql_path = library_root / "sql" / "10_template.sql"

        assert not rendered_sql_path.exists()
        assert staged_sql_path.exists()
        assert (
            staged_sql_path.read_text().strip()
            == "SELECT 'launcher-test' AS project_name"
        )

    assert not rendered_sql_path.exists()
    assert not library_root.exists()


def test_main_uses_default_pipeline_storage(monkeypatch, tmp_path: Path) -> None:
    spec_path = tmp_path / "spark-pipeline.yml"
    write_spec(
        spec_path,
        """
        name: plain-pipeline
        storage: file:///original
        configuration: {}
        libraries: []
        """,
    )

    captured: dict[str, object] = {}

    def fake_run_pipeline(
        *, spec_path: Path, full_refresh, full_refresh_all, refresh, dry
    ) -> None:
        captured["spec_path"] = spec_path
        captured["storage"] = yaml.safe_load(spec_path.read_text())["storage"]
        captured["full_refresh"] = full_refresh
        captured["full_refresh_all"] = full_refresh_all
        captured["refresh"] = refresh
        captured["dry"] = dry

    monkeypatch.setattr(spark_pipelines, "run_pipeline", fake_run_pipeline)

    spark_pipelines.main(["--spec", str(spec_path), "--dry-run"])

    assert captured["dry"] is True
    assert captured["full_refresh"] == []
    assert captured["full_refresh_all"] is False
    assert captured["refresh"] == []
    assert (
        captured["storage"]
        == Path(spark_pipelines.DEFAULT_PIPELINE_STORAGE).resolve().as_uri()
    )
    assert captured["spec_path"].parent == spec_path.parent
    assert captured["spec_path"] != spec_path
    assert not captured["spec_path"].exists()
    assert yaml.safe_load(spec_path.read_text())["storage"] == "file:///original"
