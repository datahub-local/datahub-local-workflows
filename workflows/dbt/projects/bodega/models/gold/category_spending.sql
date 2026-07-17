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
LEFT JOIN (
    -- products is merge-by-key in dlt but the Iceberg filesystem destination
    -- falls back to append, so dedupe to the latest categorisation per key
    SELECT
        description_clean,
        supermarket,
        max_by(category, categorized_at)    AS category,
        max_by(subcategory, categorized_at) AS subcategory
    FROM {{ source('bodega_enrich', 'products') }}
    GROUP BY description_clean, supermarket
) p
    ON  i.description_clean = p.description_clean
    AND i.supermarket        = p.supermarket
GROUP BY
    i.invoice_date,
    date_trunc('week',  i.invoice_date),
    date_trunc('month', i.invoice_date),
    COALESCE(p.category, 'OTHER'),
    p.subcategory,
    i.supermarket
