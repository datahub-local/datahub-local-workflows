from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path
from typing import Sequence

from pyspark.pipelines.cli import run as run_pipeline

from .spec_utils import (
    DEFAULT_PIPELINE_STORAGE,
    default_spec_path,
    materialize_spec,
)


def parse_table_list(value: str) -> list[str]:
    return [table.strip() for table in value.split(",") if table.strip()]


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Run the Spark declarative pipeline.")
    parser.add_argument(
        "--spec",
        type=Path,
        default=default_spec_path(),
        help="Path to the spark-pipeline.yml file.",
    )
    parser.add_argument(
        "--pipeline-storage",
        default=DEFAULT_PIPELINE_STORAGE,
        help="Pipeline storage root path or URI. Local paths are converted to file:// URIs.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the pipeline graph without executing it.",
    )
    parser.add_argument(
        "--refresh",
        type=parse_table_list,
        action="extend",
        default=[],
        help="Comma-separated datasets to refresh incrementally.",
    )
    parser.add_argument(
        "--full-refresh",
        type=parse_table_list,
        action="extend",
        default=[],
        help="Comma-separated datasets to reset and recompute.",
    )
    parser.add_argument(
        "--full-refresh-all",
        action="store_true",
        help="Reset and recompute the full pipeline graph.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    with materialize_spec(args.spec, args.pipeline_storage) as spec_path:
        run_pipeline(
            spec_path=spec_path,
            full_refresh=args.full_refresh,
            full_refresh_all=args.full_refresh_all,
            refresh=args.refresh,
            dry=args.dry_run,
        )


if __name__ == "__main__":
    main()
