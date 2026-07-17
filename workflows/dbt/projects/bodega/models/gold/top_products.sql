SELECT
    i.description_clean,
    p.category,
    p.subcategory,
    p.is_weighted,
    i.supermarket,
    COUNT(DISTINCT i.invoice_number)   AS purchase_count,
    SUM(i.quantity)                    AS total_quantity,
    SUM(i.total_amount)                AS total_spent,
    AVG(i.unit_price)                  AS avg_unit_price,
    MIN(i.unit_price)                  AS min_unit_price,
    MAX(i.unit_price)                  AS max_unit_price,
    MAX(i.invoice_date)                AS last_purchased_date
FROM {{ ref('invoice_items') }} i
LEFT JOIN (
    -- products is merge-by-key in dlt but the Iceberg filesystem destination
    -- falls back to append, so dedupe to the latest categorisation per key
    SELECT
        description_clean,
        supermarket,
        max_by(category, categorized_at)    AS category,
        max_by(subcategory, categorized_at) AS subcategory,
        max_by(is_weighted, categorized_at) AS is_weighted
    FROM {{ source('bodega_enrich', 'products') }}
    GROUP BY description_clean, supermarket
) p
    ON  i.description_clean = p.description_clean
    AND i.supermarket        = p.supermarket
GROUP BY i.description_clean, p.category, p.subcategory, p.is_weighted, i.supermarket
