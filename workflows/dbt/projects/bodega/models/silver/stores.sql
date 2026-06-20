SELECT
    store_vat_id                                                                    AS store_id,
    trim(upper(store_name))                                                         AS name,
    store_vat_id                                                                    AS vat_id,
    store_address                                                                   AS address,
    store_phone                                                                     AS phone,
    supermarket,
    MIN(CAST({{ bodega_parse_dt('invoice_date') }} AS DATE))                        AS first_seen_date,
    MAX(CAST({{ bodega_parse_dt('invoice_date') }} AS DATE))                        AS last_seen_date
FROM {{ source('bodega', 'raw_invoices') }}
GROUP BY store_vat_id, store_name, store_address, store_phone, supermarket
