"""Entry point for the dlt ingest/export pipelines.

Usage:
    python -m dlt_runner --pipeline <pipeline> --project <project> --target <homelab|local>

Projects and pipelines are discovered dynamically: any installed package with a module
that exposes a ``run(target)`` function is a valid project/pipeline combination.
"""

from __future__ import annotations

import argparse
import importlib
import sys


SEP = "#" * 60


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="dlt_runner")
    parser.add_argument("--pipeline", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--target", default="homelab", choices=["homelab", "local"])
    parsed = parser.parse_args(argv)

    try:
        module = importlib.import_module(f"{parsed.project}.{parsed.pipeline}")
    except ModuleNotFoundError:
        parser.error(
            f"project '{parsed.project}' does not support pipeline '{parsed.pipeline}'"
        )

    module.run(parsed.target)


if __name__ == "__main__":
    main(sys.argv[1:])
