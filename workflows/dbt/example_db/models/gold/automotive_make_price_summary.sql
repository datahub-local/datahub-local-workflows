SELECT
  make,
  COUNT(*)                       AS vehicle_count,
  ROUND(AVG(price), 2)           AS avg_price,
  ROUND(MAX(price), 2)           AS max_price,
  ROUND(AVG(horsepower), 2)      AS avg_horsepower,
  MAX(updated_date)              AS updated_date,
  CURRENT_TIMESTAMP              AS created_date
FROM {{ ref('automotive_snapshot') }}
WHERE price IS NOT NULL
GROUP BY make
