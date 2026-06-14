"""Ingest step: download the automotive CSV into the bronze medallion layer.

dlt streams the raw CSV rows (all strings, original hyphenated column names preserved via the
``direct`` naming convention) into ``bronze.example_db.automotive_source``, which dbt then
reads as a source.

- ``local``   → DuckDB file (the same ``bronze.duckdb`` the dbt local target reads).
- ``homelab`` → Iceberg table via Nessie REST + S3, with parquet staged in the temp bucket.
"""

from __future__ import annotations

import csv
import io
import os
import urllib.request

import dlt

from . import config

PIPELINE_NAME = "example_db_ingest"
DATASET_NAME = "example_db"
TABLE_NAME = "automotive_source"


@dlt.resource(name=TABLE_NAME, write_disposition="replace")
def _automotive_rows(url: str):
    with urllib.request.urlopen(url) as response:
        text = response.read().decode("utf-8")
    yield from csv.DictReader(io.StringIO(text))


def run(target: str):
    if target not in ("homelab", "local"):
        raise ValueError(f"unknown target: {target!r}")

    # Preserve the raw hyphenated CSV column names (the dbt snapshot reads them verbatim).
    os.environ.setdefault("SCHEMA__NAMING", "direct")

    resource = _automotive_rows(config.source_url())

    if target == "local":
        destination = dlt.destinations.duckdb(config.duckdb_path("bronze"))
    else:
        config.configure_iceberg_env("bronze")
        destination = dlt.destinations.filesystem(
            bucket_url=f"s3://{config.temp_bucket()}/{DATASET_NAME}"
        )
        resource.apply_hints(table_format="iceberg")

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=destination,
        dataset_name=DATASET_NAME,
    )
    return pipeline.run(resource)
