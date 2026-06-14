"""Entry point for the dlt ingest/export pipelines.

Usage:
    python -m dlt_runner --pipeline <ingest|export> --project example_db
                         --target <homelab|local>

``ingest`` populates the bronze source table dbt reads; ``export`` reverse-ETLs the
dbt-built silver/gold tables to Postgres. Both are only defined for the ``example_db``
project (the ``pi`` pipeline is pure dbt compute, no ingest/export).
"""

from __future__ import annotations

import argparse
import sys


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="dlt_runner")
    parser.add_argument("--pipeline", required=True, choices=["ingest", "export"])
    parser.add_argument("--project", required=True, choices=["example_db"])
    parser.add_argument("--target", default="homelab", choices=["homelab", "local"])
    parsed = parser.parse_args(argv)

    if parsed.pipeline == "ingest":
        from example_db import ingest

        info = ingest.run(parsed.target)
    else:
        from example_db import export

        info = export.run(parsed.target)

    print(info)


if __name__ == "__main__":
    main(sys.argv[1:])
