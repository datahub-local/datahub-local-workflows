{% import 'common/jdbc_table.sql.tpl' as db %}
{% set db_url = env.EXAMPLE_DB_URL | default('jdbc:h2:mem:testdb', true) %}
{% set schema_name = env.EXAMPLE_DB_SCHEMA | default('public', true) %}
{% set db_user = env.EXAMPLE_DB_USER | default('user', true) %}
{% set db_password = env.EXAMPLE_DB_PASSWORD | default('', true) %}

{% call db.jdbc_table(
    spark_table_name='automotive_raw_h2',
    db_url=db_url,
    schema_name=schema_name,
    table_name='automotive_raw',
    db_user=db_user,
    db_password=db_password
) %}
SELECT
  symboling,
  normalized_losses,
  make,
  fuel_type,
  aspiration,
  num_of_doors,
  body_style,
  drive_wheels,
  engine_location,
  wheel_base,
  length,
  width,
  height,
  curb_weight,
  engine_type,
  num_of_cylinders,
  engine_size,
  fuel_system,
  bore,
  stroke,
  compression_ratio,
  horsepower,
  peak_rpm,
  city_mpg,
  highway_mpg,
  price,
  updated_date,
  CURRENT_TIMESTAMP() AS created_date
FROM automotive_snapshot
{% endcall %}