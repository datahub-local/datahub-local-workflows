WITH weekly AS (
    SELECT
        description_clean,
        supermarket,
        date_trunc('week', invoice_date)  AS week_start,
        -- Blended price: unit_price on receipts is €/kg for weighted lines and
        -- €/unit otherwise, so total/quantity yields the same dimension per product.
        SUM(total_amount) / NULLIF(SUM(quantity), 0) AS price,
        SUM(total_amount)                 AS total_spent,
        SUM(quantity)                     AS total_quantity,
        COUNT(*)                          AS purchase_count
    FROM {{ ref('invoice_items') }}
    GROUP BY description_clean, supermarket, date_trunc('week', invoice_date)
),
repeat_products AS (
    SELECT description_clean, supermarket
    FROM {{ ref('invoice_items') }}
    GROUP BY description_clean, supermarket
    HAVING COUNT(*) >= 2
)
SELECT
    w.description_clean,
    w.supermarket,
    w.week_start,
    w.price,
    w.total_spent,
    w.total_quantity,
    w.purchase_count,
    p.category,
    p.subcategory,
    COALESCE(p.is_weighted, FALSE) AS is_weighted
FROM weekly w
JOIN repeat_products r
    ON  w.description_clean = r.description_clean
    AND w.supermarket        = r.supermarket
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
    ON  w.description_clean = p.description_clean
    AND w.supermarket        = p.supermarket
