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
LEFT JOIN {{ source('bodega_enrich', 'products') }} p
    ON  i.description_clean = p.description_clean
    AND i.supermarket        = p.supermarket
GROUP BY i.description_clean, p.category, p.subcategory, p.is_weighted, i.supermarket
