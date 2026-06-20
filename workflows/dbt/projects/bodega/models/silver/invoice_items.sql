SELECT
    b.invoice_number,
    CAST(_it.pos AS INTEGER)                                                       AS item_position,
    CAST({{ bodega_parse_dt('b.invoice_date') }} AS DATE)                          AS invoice_date,
    b.store_vat_id,
    b.supermarket,
    {{ bodega_json_scalar('_it.elem', '$.description') }}                          AS description_raw,
    trim(upper({{ bodega_json_scalar('_it.elem', '$.description') }}))             AS description_clean,
    CAST({{ bodega_json_scalar('_it.elem', '$.quantity') }} AS DOUBLE)             AS quantity,
    CASE
        WHEN CAST({{ bodega_json_scalar('_it.elem', '$.quantity') }} AS DOUBLE)
             != FLOOR(CAST({{ bodega_json_scalar('_it.elem', '$.quantity') }} AS DOUBLE))
        THEN 'KG' ELSE 'EA'
    END                                                                            AS unit,
    CAST({{ bodega_json_scalar('_it.elem', '$.unit_price') }} AS DOUBLE)           AS unit_price,
    CAST({{ bodega_json_scalar('_it.elem', '$.total') }} AS DOUBLE)                AS total_amount
FROM {{ source('bodega', 'raw_invoices') }} AS b
{{ bodega_explode_json('b.items_json') }}
