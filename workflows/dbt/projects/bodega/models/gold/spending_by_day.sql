SELECT
    invoice_date,
    supermarket,
    COUNT(*)              AS invoice_count,
    SUM(total_amount)     AS total_amount,
    SUM(total_tax_amount) AS total_tax,
    SUM(item_count)       AS total_items,
    AVG(total_amount)     AS avg_basket_amount
FROM {{ ref('invoices') }}
GROUP BY invoice_date, supermarket
