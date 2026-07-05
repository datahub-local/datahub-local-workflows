{#
  One row per store VAT ID. The raw invoices can carry formatting variants of the
  descriptive fields (name casing, address, phone) for the same store, so those are
  taken from the most recent invoice instead of being grouped on.
#}
WITH ranked AS (
    SELECT
        store_vat_id,
        store_name,
        store_address,
        store_phone,
        supermarket,
        CAST({{ bodega_parse_dt('invoice_date') }} AS DATE)                     AS invoice_date,
        ROW_NUMBER() OVER (
            PARTITION BY store_vat_id
            ORDER BY {{ bodega_parse_dt('invoice_date') }} DESC, invoice_number DESC
        )                                                                       AS rn
    FROM {{ source('bodega', 'raw_invoices') }}
)

SELECT
    ranked.store_vat_id                                                         AS store_id,
    trim(upper(ranked.store_name))                                              AS name,
    ranked.store_vat_id                                                         AS vat_id,
    ranked.store_address                                                        AS address,
    ranked.store_phone                                                          AS phone,
    ranked.supermarket,
    seen.first_seen_date,
    seen.last_seen_date
FROM ranked
JOIN (
    SELECT
        store_vat_id,
        MIN(invoice_date)                                                       AS first_seen_date,
        MAX(invoice_date)                                                       AS last_seen_date
    FROM ranked
    GROUP BY store_vat_id
) AS seen ON seen.store_vat_id = ranked.store_vat_id
WHERE ranked.rn = 1
