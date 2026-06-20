SELECT
    i.invoice_date,
    date_trunc('week',  i.invoice_date)  AS week_start,
    date_trunc('month', i.invoice_date)  AS month_start,
    COALESCE(p.category, 'OTHER')        AS category,
    p.subcategory,
    i.supermarket,
    COUNT(DISTINCT i.invoice_number)     AS invoice_count,
    COUNT(*)                             AS item_count,
    SUM(i.total_amount)                  AS total_spent
FROM {{ ref('invoice_items') }} i
LEFT JOIN {{ source('bodega_enrich', 'products') }} p
    ON  i.description_clean = p.description_clean
    AND i.supermarket        = p.supermarket
GROUP BY
    i.invoice_date,
    date_trunc('week',  i.invoice_date),
    date_trunc('month', i.invoice_date),
    COALESCE(p.category, 'OTHER'),
    p.subcategory,
    i.supermarket
