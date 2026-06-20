SELECT
    date_trunc('week', invoice_date)  AS week_start,
    year(invoice_date)                AS year,
    week(invoice_date)                AS week_number,
    supermarket,
    COUNT(*)                          AS invoice_count,
    SUM(total_amount)                 AS total_amount,
    SUM(total_tax_amount)             AS total_tax,
    SUM(item_count)                   AS total_items,
    AVG(total_amount)                 AS avg_basket_amount,
    MAX(total_amount)                 AS max_basket_amount
FROM {{ ref('invoices') }}
GROUP BY
    date_trunc('week', invoice_date),
    year(invoice_date),
    week(invoice_date),
    supermarket
