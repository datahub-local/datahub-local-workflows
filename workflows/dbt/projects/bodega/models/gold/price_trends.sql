WITH weekly AS (
    SELECT
        description_clean,
        supermarket,
        date_trunc('week', invoice_date)  AS week_start,
        AVG(unit_price)                   AS avg_unit_price,
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
    w.avg_unit_price,
    w.purchase_count,
    p.category,
    p.subcategory
FROM weekly w
JOIN repeat_products r USING (description_clean, supermarket)
LEFT JOIN {{ source('bodega_enrich', 'products') }} p USING (description_clean, supermarket)
