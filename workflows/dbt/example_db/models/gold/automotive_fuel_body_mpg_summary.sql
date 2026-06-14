SELECT
  fuel_type,
  body_style,
  COUNT(*)                       AS vehicle_count,
  ROUND(AVG(city_mpg), 2)        AS avg_city_mpg,
  ROUND(AVG(highway_mpg), 2)     AS avg_highway_mpg,
  ROUND(AVG(price), 2)           AS avg_price,
  MAX(updated_date)              AS updated_date,
  CURRENT_TIMESTAMP              AS created_date
FROM {{ ref('automotive_snapshot') }}
WHERE city_mpg IS NOT NULL
  AND highway_mpg IS NOT NULL
GROUP BY fuel_type, body_style
