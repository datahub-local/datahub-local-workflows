"""Integration test: run the bodega pipeline against the local (DuckDB) target.

Mirrors the production order (dlt ingest → dbt silver → dlt enrich → dbt gold) with the
two dlt steps replaced by direct seeds: it seeds the bronze ``raw_invoices`` table the dlt
ingest would produce, runs ``dbt build --select silver.*``, seeds the ``products`` table the
dlt enrich step would produce, then runs ``dbt build --select gold.*`` and asserts the
materialized medallion tables. No external services — proves the Trino-authored SQL also
runs on DuckDB via the cross_engine macros.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

# Columns the dlt ingest writes into bronze.bodega.raw_invoices.
RAW_COLUMNS = [
    "invoice_number", "invoice_date", "operator_id", "store_name", "store_vat_id",
    "store_address", "store_phone", "total_amount", "payment_method", "card_type",
    "card_number_masked", "items_json", "taxes_json", "supermarket", "_kafka_offset",
    "_ingested_at",
]

_INV1 = (
    "INV-1", "2026-01-15T18:30:00", "OP1", "Mercadona Centro", "B12345678",
    "C/ Mayor 1", "900111222", 2.90, "CARD", "VISA", "**** **** **** 1234",
    json.dumps([
        {"description": "Leche Entera", "quantity": "2",   "unit_price": "0.85", "total": "1.70"},
        {"description": "Pan Integral", "quantity": "1",   "unit_price": "1.20", "total": "1.20"},
    ]),
    json.dumps([{"rate": "10%", "base": "2.64", "tax": "0.26"}]),
    "MERCADONA", 0, "2026-01-15T18:31:00+00:00",
)
# Same store as INV-1 but with raw formatting variants (name casing, address spelling):
# the stores model must still collapse both invoices into one row per VAT ID.
_INV2 = (
    "INV-2", "2026-01-16T10:00:00", "OP1", "MERCADONA CENTRO ", "B12345678",
    "Calle Mayor, 1", "900111222", 5.25, "CASH", None, None,
    json.dumps([
        {"description": "Leche Entera",   "quantity": "3",   "unit_price": "0.85", "total": "2.55"},
        {"description": "Manzana Golden", "quantity": "1.5", "unit_price": "1.80", "total": "2.70"},
    ]),
    json.dumps([{"rate": "4%", "base": "5.05", "tax": "0.20"}]),
    "MERCADONA", 1, "2026-01-16T10:01:00+00:00",
)
RAW_ROWS = [_INV1, _INV2]

# Product dimension the dlt enrich step would categorise (silver.bodega.products).
PRODUCT_COLUMNS = ["description_clean", "supermarket", "category", "subcategory", "is_weighted"]
PRODUCT_ROWS = [
    ("LECHE ENTERA",   "MERCADONA", "DAIRY_EGGS",        "Milk",  False),
    ("PAN INTEGRAL",   "MERCADONA", "BAKERY_PASTRY",     "Bread", False),
    ("MANZANA GOLDEN", "MERCADONA", "FRUITS_VEGETABLES", "Apple", True),
]

ROOT = Path(__file__).parent.parent.parent


def _dbt_build(select: str) -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "dbt_runner", "--project", "bodega",
         "--target", "local", "--select", select, "--full-refresh"],
        cwd=ROOT,
        env={**os.environ},
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    import duckdb

    tmp = tmp_path_factory.mktemp("dbt_bodega_integration")
    paths = {
        "DBT_DUCKDB_PATH": str(tmp / "bronze.duckdb"),
        "DBT_DUCKDB_SILVER_PATH": str(tmp / "silver.duckdb"),
        "DBT_DUCKDB_GOLD_PATH": str(tmp / "gold.duckdb"),
    }
    os.environ.update(paths)

    # 1. Seed the bronze source the dlt ingest would create.
    cols = ", ".join(
        f'"{c}" ' + ("DOUBLE" if c == "total_amount" else "BIGINT" if c == "_kafka_offset" else "VARCHAR")
        for c in RAW_COLUMNS
    )
    placeholders = ", ".join("?" for _ in RAW_COLUMNS)
    bronze = duckdb.connect(paths["DBT_DUCKDB_PATH"])
    bronze.execute("CREATE SCHEMA IF NOT EXISTS bodega")
    bronze.execute(f"CREATE OR REPLACE TABLE bodega.raw_invoices ({cols})")
    bronze.executemany(f"INSERT INTO bodega.raw_invoices VALUES ({placeholders})", RAW_ROWS)
    bronze.close()

    # 2. Build the silver layer (in its own process — DuckDB single-writer per file).
    _dbt_build("silver.*")

    # 3. Seed the product dimension the dlt enrich step would create.
    p_cols = ", ".join(f'"{c}" ' + ("BOOLEAN" if c == "is_weighted" else "VARCHAR") for c in PRODUCT_COLUMNS)
    p_ph = ", ".join("?" for _ in PRODUCT_COLUMNS)
    silver = duckdb.connect(paths["DBT_DUCKDB_SILVER_PATH"])
    silver.execute("CREATE SCHEMA IF NOT EXISTS bodega")
    silver.execute(f"CREATE OR REPLACE TABLE bodega.products ({p_cols})")
    silver.executemany(f"INSERT INTO bodega.products VALUES ({p_ph})", PRODUCT_ROWS)
    silver.close()

    # 4. Build the gold layer.
    _dbt_build("gold.*")

    yield paths


@pytest.fixture(scope="module")
def con(warehouse):
    import duckdb

    verify = duckdb.connect(warehouse["DBT_DUCKDB_PATH"], read_only=True)
    verify.execute(f"ATTACH '{warehouse['DBT_DUCKDB_SILVER_PATH']}' AS silver (READ_ONLY)")
    verify.execute(f"ATTACH '{warehouse['DBT_DUCKDB_GOLD_PATH']}' AS gold (READ_ONLY)")
    yield verify
    verify.close()


def _count(con, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def test_silver_invoices_one_row_per_invoice(con):
    assert _count(con, "silver.bodega.invoices") == len(RAW_ROWS)


def test_silver_invoices_computes_tax_from_json(con):
    tax = con.execute(
        "SELECT total_tax_amount FROM silver.bodega.invoices WHERE invoice_number = 'INV-1'"
    ).fetchone()[0]
    assert tax == pytest.approx(0.26)


def test_silver_invoices_item_count_from_json(con):
    counts = con.execute(
        "SELECT invoice_number, item_count FROM silver.bodega.invoices ORDER BY invoice_number"
    ).fetchall()
    assert counts == [("INV-1", 2), ("INV-2", 2)]


def test_silver_invoice_items_exploded_with_position(con):
    assert _count(con, "silver.bodega.invoice_items") == 4
    positions = con.execute(
        "SELECT DISTINCT item_position FROM silver.bodega.invoice_items ORDER BY item_position"
    ).fetchall()
    assert positions == [(1,), (2,)]


def test_silver_invoice_items_derives_unit(con):
    # quantity 1.5 (Manzana) is fractional -> KG; whole quantities -> EA.
    unit = con.execute(
        "SELECT unit FROM silver.bodega.invoice_items WHERE description_clean = 'MANZANA GOLDEN'"
    ).fetchone()[0]
    assert unit == "KG"


def test_silver_invoice_taxes_exploded(con):
    assert _count(con, "silver.bodega.invoice_taxes") == 2


def test_silver_stores_dedups_by_vat(con):
    assert _count(con, "silver.bodega.stores") == 1


def test_silver_stores_latest_invoice_wins(con):
    # INV-2 is the most recent invoice, so its descriptive fields win.
    row = con.execute(
        "SELECT name, address, first_seen_date, last_seen_date FROM silver.bodega.stores"
    ).fetchone()
    assert row[0] == "MERCADONA CENTRO"
    assert row[1] == "Calle Mayor, 1"
    assert str(row[2]) == "2026-01-15"
    assert str(row[3]) == "2026-01-16"


def test_gold_top_products_joins_categories(con):
    rows = con.execute(
        "SELECT description_clean, category FROM gold.bodega.top_products ORDER BY description_clean"
    ).fetchall()
    assert ("LECHE ENTERA", "DAIRY_EGGS") in rows
    assert len(rows) == 3


def test_gold_price_trends_keeps_only_repeat_products(con):
    # Only Leche Entera is bought in both invoices (>= 2 line items).
    products = {r[0] for r in con.execute(
        "SELECT DISTINCT description_clean FROM gold.bodega.price_trends"
    ).fetchall()}
    assert products == {"LECHE ENTERA"}


def test_gold_category_spending_in_gold_catalog(con):
    assert _count(con, "gold.bodega.category_spending") > 0


def test_gold_tax_summary_sums_by_month(con):
    assert _count(con, "gold.bodega.tax_summary") == 2
