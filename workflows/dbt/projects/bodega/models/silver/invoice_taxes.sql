SELECT
    b.invoice_number,
    b.supermarket,
    CAST({{ bodega_parse_dt('b.invoice_date') }} AS DATE)              AS invoice_date,
    {{ bodega_json_scalar('_it.elem', '$.rate') }}                     AS tax_rate,
    CAST({{ bodega_json_scalar('_it.elem', '$.base') }} AS DOUBLE)     AS base_amount,
    CAST({{ bodega_json_scalar('_it.elem', '$.tax')  }} AS DOUBLE)     AS tax_amount
FROM {{ source('bodega', 'raw_invoices') }} AS b
{{ bodega_explode_json('b.taxes_json') }}
