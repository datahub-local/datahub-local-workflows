SELECT
    date_trunc('month', invoice_date)  AS month_start,
    tax_rate,
    supermarket,
    SUM(base_amount)                   AS base_amount,
    SUM(tax_amount)                    AS tax_amount
FROM {{ ref('invoice_taxes') }}
GROUP BY date_trunc('month', invoice_date), tax_rate, supermarket
