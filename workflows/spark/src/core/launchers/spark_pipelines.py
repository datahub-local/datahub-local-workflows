from __future__ import annotations

import os
import sys
from argparse import ArgumentParser
from contextlib import contextmanager
from pathlib import Path
from typing import Sequence

from pyspark.pipelines.cli import run as run_pipeline

from core.launchers import remove_pex_arg
from core.launchers.spec_utils import (
    DEFAULT_PIPELINE_STORAGE,
    default_spec_file,
    materialize_spec,
    resolve_spec_path,
)

# Generate env var prefix from module name
_MODULE_NAME = Path(__file__).stem.upper()
_ENV_PREFIX = f"{_MODULE_NAME}_"


def get_env_var(name: str, default: str | None = None) -> str | None:
    """Get environment variable with module prefix."""
    env_var_name = f"{_ENV_PREFIX}{name.upper()}"
    return os.environ.get(env_var_name, default)


def parse_table_list(value: str) -> list[str]:
    return [table.strip() for table in value.split(",") if table.strip()]


@contextmanager
def default_env_var(name: str, value: str):
    existing_value = os.environ.get(name)
    if existing_value is not None:
        yield
        return

    os.environ[name] = value
    try:
        yield
    finally:
        os.environ.pop(name, None)


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description="Run the Spark declarative pipeline.",
        epilog=f"Environment variables (prefix: {_ENV_PREFIX}): PIPELINE, SPEC, PIPELINE_STORAGE, DRY_RUN, REFRESH, FULL_REFRESH, FULL_REFRESH_ALL",
    )
    parser.add_argument(
        "--pipeline",
        type=str,
        required=get_env_var("pipeline") is None,
        default=get_env_var("pipeline"),
        help="Name of the pipeline (corresponds to the pipelines folder). Can be set via SPARK_PIPELINES_PIPELINE.",
    )
    parser.add_argument(
        "--spec",
        type=str,
        default=get_env_var("spec") or default_spec_file(),
        help="Basename of the spark pipeline spec file (e.g., 'spark-pipeline.yml'). Can be set via SPARK_PIPELINES_SPEC.",
    )
    parser.add_argument(
        "--pipeline-storage",
        default=get_env_var("pipeline_storage") or DEFAULT_PIPELINE_STORAGE,
        help="Pipeline storage root path or URI. Local paths are converted to file:// URIs. Can be set via SPARK_PIPELINES_PIPELINE_STORAGE.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=(get_env_var("dry_run") or "").lower() in ("true", "1", "yes"),
        help="Validate the pipeline graph without executing it. Can be set via SPARK_PIPELINES_DRY_RUN.",
    )
    parser.add_argument(
        "--refresh",
        type=parse_table_list,
        action="extend",
        default=parse_table_list(get_env_var("refresh") or ""),
        help="Comma-separated datasets to refresh incrementally. Can be set via SPARK_PIPELINES_REFRESH.",
    )
    parser.add_argument(
        "--full-refresh",
        type=parse_table_list,
        action="extend",
        default=parse_table_list(get_env_var("full_refresh") or ""),
        help="Comma-separated datasets to reset and recompute. Can be set via SPARK_PIPELINES_FULL_REFRESH.",
    )
    parser.add_argument(
        "--full-refresh-all",
        action="store_true",
        default=(get_env_var("full_refresh_all") or "").lower()
        in ("true", "1", "yes"),
        help="Reset and recompute the full pipeline graph. Can be set via SPARK_PIPELINES_FULL_REFRESH_ALL.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    spec_path = resolve_spec_path(args.pipeline, args.spec)
    with materialize_spec(spec_path, args.pipeline_storage) as materialized_spec_path:
        with default_env_var("SPARK_API_MODE", "connect"):
            run_pipeline(
                spec_path=materialized_spec_path,
                full_refresh=args.full_refresh,
                full_refresh_all=args.full_refresh_all,
                refresh=args.refresh,
                dry=args.dry_run,
            )


if __name__ == "__main__":
    argv = remove_pex_arg(sys.argv, __file__.split("/")[-1])
    main(argv)
