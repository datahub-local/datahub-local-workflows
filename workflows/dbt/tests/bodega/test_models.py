"""Inspect bodega model SQL for correct refs, sources, and key columns."""
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent.parent / "projects" / "bodega"
MODELS_DIR  = PROJECT_DIR / "models"


class TestSilverInvoices:
    def setup_method(self):
        self.sql = (MODELS_DIR / "silver" / "invoices.sql").read_text()

    def test_reads_from_bronze_source(self):
        assert "source('bodega', 'raw_invoices')" in self.sql

    def test_parses_invoice_date(self):
        assert "bodega_parse_dt" in self.sql
        assert "invoice_date" in self.sql

    def test_exposes_time_dimensions(self):
        lower = self.sql.lower()
        for col in ("invoice_year", "invoice_month", "invoice_week"):
            assert col in lower, f"missing column {col}"

    def test_computes_tax_amounts_from_json(self):
        assert "taxes_json" in self.sql
        assert "total_tax_amount" in self.sql
        assert "total_base_amount" in self.sql

    def test_exposes_item_count(self):
        assert "item_count" in self.sql
        assert "bodega_json_len" in self.sql


class TestSilverInvoiceItems:
    def setup_method(self):
        self.sql = (MODELS_DIR / "silver" / "invoice_items.sql").read_text()

    def test_reads_from_bronze_source(self):
        assert "source('bodega', 'raw_invoices')" in self.sql

    def test_unnests_items_json(self):
        assert "items_json" in self.sql
        assert "bodega_explode_json" in self.sql
        assert "_it.pos" in self.sql

    def test_extracts_item_fields(self):
        lower = self.sql.lower()
        for col in ("description_raw", "description_clean", "quantity", "unit", "unit_price", "total_amount"):
            assert col in lower, f"missing column {col}"

    def test_derives_kg_vs_ea_unit(self):
        assert "KG" in self.sql
        assert "EA" in self.sql
        assert "FLOOR" in self.sql


class TestSilverInvoiceTaxes:
    def setup_method(self):
        self.sql = (MODELS_DIR / "silver" / "invoice_taxes.sql").read_text()

    def test_reads_from_bronze_source(self):
        assert "source('bodega', 'raw_invoices')" in self.sql

    def test_unnests_taxes_json(self):
        assert "taxes_json" in self.sql
        assert "bodega_explode_json" in self.sql

    def test_extracts_tax_fields(self):
        lower = self.sql.lower()
        for col in ("tax_rate", "base_amount", "tax_amount"):
            assert col in lower, f"missing column {col}"


class TestSilverStores:
    def setup_method(self):
        self.sql = (MODELS_DIR / "silver" / "stores.sql").read_text()

    def test_reads_from_bronze_source(self):
        assert "source('bodega', 'raw_invoices')" in self.sql

    def test_groups_by_vat_id(self):
        assert "store_vat_id" in self.sql
        assert "GROUP BY" in self.sql.upper()

    def test_exposes_first_and_last_seen(self):
        lower = self.sql.lower()
        assert "first_seen_date" in lower
        assert "last_seen_date" in lower


class TestGoldSpendingByDay:
    def setup_method(self):
        self.sql = (MODELS_DIR / "gold" / "spending_by_day.sql").read_text()

    def test_refs_invoices(self):
        assert "ref('invoices')" in self.sql

    def test_exposes_key_metrics(self):
        lower = self.sql.lower()
        for col in ("invoice_count", "total_amount", "total_tax", "total_items", "avg_basket_amount"):
            assert col in lower, f"missing column {col}"


class TestGoldSpendingByWeek:
    def setup_method(self):
        self.sql = (MODELS_DIR / "gold" / "spending_by_week.sql").read_text()

    def test_refs_invoices(self):
        assert "ref('invoices')" in self.sql

    def test_uses_week_truncation(self):
        assert "date_trunc" in self.sql
        assert "week" in self.sql
        assert "week_start" in self.sql


class TestGoldTopProducts:
    def setup_method(self):
        self.sql = (MODELS_DIR / "gold" / "top_products.sql").read_text()

    def test_refs_invoice_items(self):
        assert "ref('invoice_items')" in self.sql

    def test_joins_products_source(self):
        assert "source('bodega_enrich', 'products')" in self.sql

    def test_exposes_purchase_and_spend_metrics(self):
        lower = self.sql.lower()
        for col in ("purchase_count", "total_spent", "avg_unit_price"):
            assert col in lower, f"missing column {col}"


class TestGoldPriceTrends:
    def setup_method(self):
        self.sql = (MODELS_DIR / "gold" / "price_trends.sql").read_text()

    def test_refs_invoice_items(self):
        assert "ref('invoice_items')" in self.sql

    def test_joins_products_source(self):
        assert "source('bodega_enrich', 'products')" in self.sql

    def test_filters_to_repeat_products(self):
        assert "HAVING" in self.sql.upper()
        assert ">= 2" in self.sql


class TestGoldCategorySpending:
    def setup_method(self):
        self.sql = (MODELS_DIR / "gold" / "category_spending.sql").read_text()

    def test_refs_invoice_items(self):
        assert "ref('invoice_items')" in self.sql

    def test_joins_products_source(self):
        assert "source('bodega_enrich', 'products')" in self.sql

    def test_coalesces_category_to_other(self):
        assert "COALESCE" in self.sql
        assert "'OTHER'" in self.sql

    def test_exposes_time_granularities(self):
        assert "week_start" in self.sql
        assert "month_start" in self.sql


class TestGoldTaxSummary:
    def setup_method(self):
        self.sql = (MODELS_DIR / "gold" / "tax_summary.sql").read_text()

    def test_refs_invoice_taxes(self):
        assert "ref('invoice_taxes')" in self.sql

    def test_truncates_to_month(self):
        assert "date_trunc" in self.sql
        assert "month_start" in self.sql

    def test_sums_tax_components(self):
        lower = self.sql.lower()
        assert "base_amount" in lower
        assert "tax_amount" in lower
