{% set source_url = env.EXAMPLE_DB_SOURCE_URL | default('https://raw.githubusercontent.com/Opensourcefordatascience/Data-sets/refs/heads/master/automotive_data.csv', true) %}

CREATE OR REPLACE TEMP VIEW automotive_source_csv
USING csv
OPTIONS (
  path '{{ source_url }}',
  header 'true',
  inferSchema 'false',
  mode 'PERMISSIVE'
);

CREATE OR REPLACE TEMP VIEW automotive_snapshot AS
SELECT
  CAST(NULLIF(TRIM(`symboling`), '?') AS INT) AS symboling,
  CAST(NULLIF(TRIM(`normalized-losses`), '?') AS INT) AS normalized_losses,
  TRIM(`make`) AS make,
  TRIM(`fuel-type`) AS fuel_type,
  TRIM(`aspiration`) AS aspiration,
  TRIM(`num-of-doors`) AS num_of_doors,
  TRIM(`body-style`) AS body_style,
  TRIM(`drive-wheels`) AS drive_wheels,
  TRIM(`engine-location`) AS engine_location,
  CAST(NULLIF(TRIM(`wheel-base`), '?') AS DOUBLE) AS wheel_base,
  CAST(NULLIF(TRIM(`length`), '?') AS DOUBLE) AS length,
  CAST(NULLIF(TRIM(`width`), '?') AS DOUBLE) AS width,
  CAST(NULLIF(TRIM(`height`), '?') AS DOUBLE) AS height,
  CAST(NULLIF(TRIM(`curb-weight`), '?') AS INT) AS curb_weight,
  TRIM(`engine-type`) AS engine_type,
  TRIM(`num-of-cylinders`) AS num_of_cylinders,
  CAST(NULLIF(TRIM(`engine-size`), '?') AS INT) AS engine_size,
  TRIM(`fuel-system`) AS fuel_system,
  CAST(NULLIF(TRIM(`bore`), '?') AS DOUBLE) AS bore,
  CAST(NULLIF(TRIM(`stroke`), '?') AS DOUBLE) AS stroke,
  CAST(NULLIF(TRIM(`compression-ratio`), '?') AS DOUBLE) AS compression_ratio,
  CAST(NULLIF(TRIM(`horsepower`), '?') AS INT) AS horsepower,
  CAST(NULLIF(TRIM(`peak-rpm`), '?') AS INT) AS peak_rpm,
  CAST(NULLIF(TRIM(`city-mpg`), '?') AS INT) AS city_mpg,
  CAST(NULLIF(TRIM(`highway-mpg`), '?') AS INT) AS highway_mpg,
  CAST(NULLIF(TRIM(`price`), '?') AS DOUBLE) AS price,
  CURRENT_TIMESTAMP() AS updated_date
FROM automotive_source_csv
WHERE COALESCE(TRIM(`make`), '') <> '';