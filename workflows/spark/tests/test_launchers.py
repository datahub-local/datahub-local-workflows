import os

from core.launchers import remove_pex_arg
from core.launchers.spark_pipelines import _ENV_PREFIX, build_parser, get_env_var
from core.launchers.spec_utils import (
    DEFAULT_SPEC_FILE,
    SPEC_FILE_VARIANTS,
    default_spec_file,
    resolve_spec_path,
)


class TestRemovePexArg:
    """Test suite for remove_pex_arg function."""

    def test_removes_pex_file_path(self):
        """Test that the first element (pex file path) is stripped from argv."""
        argv = ["app.pex", "arg1", "arg2"]
        result = remove_pex_arg(argv, "script.py")
        assert result == ["arg1", "arg2"]

    def test_filters_pex_prefixed_args(self):
        """Test that args starting with --pex- are filtered out."""
        argv = ["app.pex", "--pex-root=/path", "arg1"]
        result = remove_pex_arg(argv, "script.py")
        assert result == ["arg1"]

    def test_filters_multiple_pex_prefixed_args(self):
        """Test that multiple --pex- args are all removed."""
        argv = ["app.pex", "--pex-root=/path", "--pex-cache=/cache", "arg1", "arg2"]
        result = remove_pex_arg(argv, "script.py")
        assert result == ["arg1", "arg2"]

    def test_only_pex_args_returns_empty(self):
        """Test that only --pex- args after pex file results in empty list."""
        argv = ["app.pex", "--pex-root=/path", "--pex-cache=/cache"]
        result = remove_pex_arg(argv, "script.py")
        assert result == []

    def test_single_element_returns_empty(self):
        """Test that argv with only the pex file path returns empty list."""
        argv = ["app.pex"]
        result = remove_pex_arg(argv, "script.py")
        assert result == []

    def test_empty_argv_returns_empty(self):
        """Test that empty argv returns empty list."""
        argv = []
        result = remove_pex_arg(argv, "script.py")
        assert result == []

    def test_non_pex_prefixed_args_are_kept(self):
        """Test that args not containing 'pex' are preserved."""
        argv = ["app.pex", "--pex-arg", "arg1", "--pex-root=/path"]
        result = remove_pex_arg(argv, "script.py")
        assert result == ["arg1"]

    def test_script_name_not_removed(self):
        """Test that script_name is not filtered out (unused parameter)."""
        argv = ["app.pex", "script.py", "arg1"]
        result = remove_pex_arg(argv, "script.py")
        assert result == ["script.py", "arg1"]


class TestDefaultSpecFile:
    """Test suite for default_spec_file function."""

    def test_default_spec_file_returns_string(self):
        """Test that default_spec_file returns the default spec filename."""
        result = default_spec_file()
        assert result == DEFAULT_SPEC_FILE
        assert isinstance(result, str)

    def test_default_spec_file_value(self):
        """Test that default spec file is spark-pipeline.yml."""
        result = default_spec_file()
        assert result == "spark-pipeline.yml"


class TestResolveSpecPath:
    """Test suite for resolve_spec_path function."""

    def test_resolve_spec_path_basic(self):
        """Test that resolve_spec_path constructs the correct path with variant detection."""
        result = resolve_spec_path("pi", "spark-pipeline.yml")
        # When "spark-pipeline.yml" is requested but a variant exists, the variant is returned
        assert "pipelines/pi/spark-pipeline" in str(result)
        assert "pipelines" in str(result)
        assert "pi" in str(result)

    def test_resolve_spec_path_different_pipeline(self):
        """Test resolving spec path with different pipeline name."""
        result = resolve_spec_path("example-db", "spark-pipeline.yml.j2")
        assert "example-db" in str(result)
        assert "spark-pipeline.yml.j2" in str(result)

    def test_resolve_spec_path_includes_pipelines_folder(self):
        """Test that resolved path includes pipelines folder."""
        result = resolve_spec_path("test_pipeline", "spec.yml")
        result_str = str(result)
        assert "pipelines" in result_str
        assert "test_pipeline" in result_str
        assert "spec.yml" in result_str

    def test_resolve_spec_path_is_path_object(self):
        """Test that resolve_spec_path returns a Path object."""
        from pathlib import Path

        result = resolve_spec_path("pi", "spark-pipeline.yml")
        assert isinstance(result, Path)


class TestSpecFileVariants:
    """Test suite for spec file variant detection."""

    def test_spec_file_variants_contains_all_expected_formats(self):
        """Test that SPEC_FILE_VARIANTS includes all supported formats."""
        expected_variants = [
            "spark-pipeline.yml",
            "spark-pipeline.yaml",
            "spark-pipeline.yml.j2",
            "spark-pipeline.yaml.j2",
        ]
        for variant in expected_variants:
            assert variant in SPEC_FILE_VARIANTS, f"{variant} not in SPEC_FILE_VARIANTS"

    def test_spec_file_variants_count(self):
        """Test that all expected variants are present."""
        assert len(SPEC_FILE_VARIANTS) == 4

    def test_resolve_spec_path_with_base_name_for_existing_file(self):
        """Test that resolve_spec_path finds spark-pipeline.yml.j2 in pi pipeline."""
        # The actual pi/spark-pipeline.yml.j2 file should exist
        result = resolve_spec_path("pi", "spark-pipeline.yml.j2")
        assert result.exists(), f"Expected spec file not found at {result}"

    def test_resolve_spec_path_returns_exact_match_if_exists(self):
        """Test that exact file matches are returned directly."""
        result = resolve_spec_path("pi", "spark-pipeline.yml.j2")
        assert "spark-pipeline.yml.j2" in str(result)
        assert result.exists()


class TestEnvVarPrefix:
    """Test suite for environment variable prefix functionality."""

    def test_env_prefix_from_module_name(self):
        """Test that _ENV_PREFIX is correctly set to SPARK_PIPELINES_."""
        assert _ENV_PREFIX == "SPARK_PIPELINES_"

    def test_get_env_var_with_prefix(self):
        """Test that get_env_var correctly adds prefix."""
        os.environ["SPARK_PIPELINES_TEST_VAR"] = "test_value"
        try:
            result = get_env_var("test_var")
            assert result == "test_value"
        finally:
            del os.environ["SPARK_PIPELINES_TEST_VAR"]

    def test_get_env_var_returns_none_if_not_set(self):
        """Test that get_env_var returns None for unset variables."""
        result = get_env_var("nonexistent_var_xyz")
        assert result is None

    def test_get_env_var_with_default(self):
        """Test that get_env_var uses default when env var not set."""
        result = get_env_var("nonexistent_var_xyz", "default_value")
        assert result == "default_value"

    def test_get_env_var_case_insensitive_lookup(self):
        """Test that get_env_var converts to uppercase."""
        os.environ["SPARK_PIPELINES_PIPELINE_NAME"] = "my_pipeline"
        try:
            result = get_env_var("pipeline_name")
            assert result == "my_pipeline"
        finally:
            del os.environ["SPARK_PIPELINES_PIPELINE_NAME"]


class TestSparkPipelinesParser:
    """Test suite for build_parser with environment variable support."""

    def test_parser_with_env_var_pipeline(self):
        """Test that parser uses SPARK_PIPELINES_PIPELINE from environment."""
        os.environ["SPARK_PIPELINES_PIPELINE"] = "pi"
        try:
            parser = build_parser()
            args = parser.parse_args([])
            assert args.pipeline == "pi"
        finally:
            del os.environ["SPARK_PIPELINES_PIPELINE"]

    def test_parser_command_line_overrides_env_var(self):
        """Test that command-line args override environment variables."""
        os.environ["SPARK_PIPELINES_PIPELINE"] = "pi"
        try:
            parser = build_parser()
            args = parser.parse_args(["--pipeline", "example-db"])
            assert args.pipeline == "example-db"
        finally:
            del os.environ["SPARK_PIPELINES_PIPELINE"]

    def test_parser_with_env_var_spec(self):
        """Test that parser uses SPARK_PIPELINES_SPEC from environment."""
        os.environ["SPARK_PIPELINES_SPEC"] = "spark-pipeline.yaml.j2"
        os.environ["SPARK_PIPELINES_PIPELINE"] = "pi"
        try:
            parser = build_parser()
            args = parser.parse_args([])
            assert args.spec == "spark-pipeline.yaml.j2"
        finally:
            del os.environ["SPARK_PIPELINES_SPEC"]
            del os.environ["SPARK_PIPELINES_PIPELINE"]

    def test_parser_with_env_var_dry_run(self):
        """Test that parser recognizes SPARK_PIPELINES_DRY_RUN as true."""
        os.environ["SPARK_PIPELINES_DRY_RUN"] = "true"
        os.environ["SPARK_PIPELINES_PIPELINE"] = "pi"
        try:
            parser = build_parser()
            args = parser.parse_args([])
            assert args.dry_run is True
        finally:
            del os.environ["SPARK_PIPELINES_DRY_RUN"]
            del os.environ["SPARK_PIPELINES_PIPELINE"]

    def test_parser_dry_run_env_var_false_values(self):
        """Test that various falsy values are recognized."""
        for falsy_value in ["false", "0", "no"]:
            os.environ["SPARK_PIPELINES_DRY_RUN"] = falsy_value
            os.environ["SPARK_PIPELINES_PIPELINE"] = "pi"
            try:
                parser = build_parser()
                args = parser.parse_args([])
                assert args.dry_run is False, f"Failed for value: {falsy_value}"
            finally:
                del os.environ["SPARK_PIPELINES_DRY_RUN"]
                del os.environ["SPARK_PIPELINES_PIPELINE"]

    def test_parser_with_env_var_pipeline_storage(self):
        """Test that parser uses SPARK_PIPELINES_PIPELINE_STORAGE from environment."""
        os.environ["SPARK_PIPELINES_PIPELINE_STORAGE"] = "/custom/path"
        os.environ["SPARK_PIPELINES_PIPELINE"] = "pi"
        try:
            parser = build_parser()
            args = parser.parse_args([])
            assert args.pipeline_storage == "/custom/path"
        finally:
            del os.environ["SPARK_PIPELINES_PIPELINE_STORAGE"]
            del os.environ["SPARK_PIPELINES_PIPELINE"]
