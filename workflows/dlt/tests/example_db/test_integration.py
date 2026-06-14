"""Integration test: run the local (DuckDB) dlt ingest and export end to end.

Ingest reads a local CSV (``file://``) into the bronze catalog; export reads seeded
silver/gold catalogs (what dbt would build) and writes them to the export DuckDB file.
No network, no Trino/Polaris/Postgres — slower than the config unit tests, so kept separate.
"""
import os

import pytest

CSV = (
    "symboling,normalized-losses,make,fuel-type,city-mpg,highway-mpg,price\n"
    "3,?,alfa-romero,gas,21,27,13495\n"
    "1,118,audi,gas,24,30,13950\n"
)


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("dlt_integration")
    csv_path = tmp / "automotive.csv"
    csv_path.write_text(CSV)

    os.environ.update({
        "DBT_DUCKDB_PATH": str(tmp / "bronze.duckdb"),
        "DBT_DUCKDB_SILVER_PATH": str(tmp / "silver.duckdb"),
        "DBT_DUCKDB_GOLD_PATH": str(tmp / "gold.duckdb"),
        "DLT_EXPORT_DUCKDB_PATH": str(tmp / "export.duckdb"),
        "EXAMPLE_DB_SOURCE_URL": csv_path.as_uri(),
        "EXAMPLE_DB_SCHEMA": "public",
    })
    return tmp


def _query(path, sql):
    import duckdb

    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(sql).fetchone()
    finally:
        con.close()


def test_ingest_writes_bronze_source(warehouse):
    from example_db import ingest

    ingest.run("local")
    row = _query(warehouse / "bronze.duckdb", "SELECT COUNT(*) FROM example_db.automotive_source")
    assert row[0] == 2
    # Hyphenated CSV column names are preserved verbatim.
    cols = _query(
        warehouse / "bronze.duckdb",
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema='example_db' AND column_name='normalized-losses'",
    )
    assert cols[0] == 1


def test_export_reads_silver_gold_and_writes_target(warehouse):
    import duckdb

    # Seed the silver/gold tables dbt would normally build.
    silver = duckdb.connect(str(warehouse / "silver.duckdb"))
    silver.execute("CREATE SCHEMA IF NOT EXISTS example_db")
    silver.execute(
        "CREATE OR REPLACE TABLE example_db.automotive_raw AS "
        "SELECT * FROM (VALUES ('audi', 13950.0), ('bmw', 16430.0)) AS t(make, price)"
    )
    silver.close()

    gold = duckdb.connect(str(warehouse / "gold.duckdb"))
    gold.execute("CREATE SCHEMA IF NOT EXISTS example_db")
    gold.execute(
        "CREATE OR REPLACE TABLE example_db.automotive_make_price_summary AS "
        "SELECT * FROM (VALUES ('audi', 1)) AS t(make, vehicle_count)"
    )
    gold.execute(
        "CREATE OR REPLACE TABLE example_db.automotive_fuel_body_mpg_summary AS "
        "SELECT * FROM (VALUES ('gas', 'sedan', 1)) AS t(fuel_type, body_style, vehicle_count)"
    )
    gold.close()

    from example_db import export

    export.run("local")

    export_db = warehouse / "export.duckdb"
    assert _query(export_db, "SELECT COUNT(*) FROM public.automotive_raw")[0] == 2
    assert _query(export_db, "SELECT COUNT(*) FROM public.automotive_make_price_summary")[0] == 1
    assert _query(export_db, "SELECT COUNT(*) FROM public.automotive_fuel_body_mpg_summary")[0] == 1
