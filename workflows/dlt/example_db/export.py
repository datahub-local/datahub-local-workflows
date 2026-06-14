"""Export step: reverse-ETL the dbt-built silver/gold tables to Postgres.

Each configured medallion table is read from the warehouse and loaded into the external
database under ``EXAMPLE_DB_SCHEMA``.

- ``local``   → reads the DuckDB catalog files, writes to a DuckDB ``export`` file (stands in
  for Postgres so the flow is testable without a server).
- ``homelab`` → reads Trino (Iceberg), writes to Postgres, staging parquet in the temp bucket.
"""

from __future__ import annotations

import dlt

from . import config

PIPELINE_NAME = "example_db_export"


def _read_table(target: str, catalog: str, schema: str, table: str):
    if target == "local":
        import duckdb

        con = duckdb.connect(config.duckdb_path(catalog), read_only=True)
        try:
            cur = con.execute(f'SELECT * FROM "{schema}"."{table}"')
            columns = [d[0] for d in cur.description]
            for row in cur.fetchall():
                yield dict(zip(columns, row))
        finally:
            con.close()
    else:
        from sqlalchemy import create_engine, text

        engine = create_engine(config.trino_url())
        with engine.connect() as conn:
            result = conn.execute(text(f'SELECT * FROM "{catalog}"."{schema}"."{table}"'))
            columns = list(result.keys())
            for row in result:
                yield dict(zip(columns, row))


def run(target: str):
    if target not in ("homelab", "local"):
        raise ValueError(f"unknown target: {target!r}")

    if target == "local":
        destination = dlt.destinations.duckdb(config.export_duckdb_path())
    else:
        destination = dlt.destinations.postgres(config.postgres_dsn())

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=destination,
        dataset_name=config.export_schema(),
    )

    resources = [
        dlt.resource(
            _read_table(target, catalog, schema, table),
            name=target_table,
            write_disposition="replace",
        )
        for (catalog, schema, table), target_table in config.EXPORTS.items()
    ]
    return pipeline.run(resources)
