# Bodega dashboard

Five-tab Superset dashboard over the bodega dbt gold layer
(`workflows/dbt/projects/bodega`), reading Trino catalog `gold`, schema `bodega`.

| Tab | Charts | Datasets |
|---|---|---|
| Overview | 4 KPI tiles (spent, trips, avg basket, items) · weekly spend line · avg basket by weekday · monthly spend stacked by supermarket | `spending_by_day`, `spending_by_week` |
| Categories | monthly spend stacked by category · category totals (horizontal bar) · top subcategories table | `category_spending` |
| Products | top products by spend (horizontal bar) · most frequently bought table | `top_products` |
| Prices | staple price index line (top 5 by purchases) · basket price index KPI · biggest price moves table | `price_index`, `price_movers` (virtual) |
| Taxes | monthly VAT stacked by rate · VAT totals table | `tax_summary` |

Native filters (scope: all tabs): date range (default *Last year*), supermarket,
category.

Notes:

- `price_index` and `price_movers` are **virtual datasets** over
  `gold.bodega.price_trends`: unit prices span an order of magnitude across
  products, so trends are indexed to 100 at each product's first observed week
  instead of plotted on raw axes.
- The `weekday` column on `spending_by_day` is a calculated column
  (`'1 Mon' … '7 Sun'`) so weekday bars sort chronologically.
- The average basket metric is `SUM(total_amount) / SUM(invoice_count)` — never
  an average of daily averages.
