{% import 'common/jdbc_table.sql.tpl' as db %}
{% set db_url = env.EXAMPLE_DB_URL | default('jdbc:h2:mem:testdb', true) %}
{% set schema_name = env.EXAMPLE_DB_SCHEMA | default('public', true) %}
{% set db_user = env.EXAMPLE_DB_USER | default('user', true) %}
{% set db_password = env.EXAMPLE_DB_PASSWORD | default('', true) %}

{% call db.jdbc_table(
    spark_table_name='automotive_fuel_body_mpg_summary_h2',
    db_url=db_url,
    schema_name=schema_name,
    table_name='automotive_fuel_body_mpg_summary',
    db_user=db_user,
    db_password=db_password
) %}
SELECT
  fuel_type,
  body_style,
  COUNT(*) AS vehicle_count,
  ROUND(AVG(city_mpg), 2) AS avg_city_mpg,
  ROUND(AVG(highway_mpg), 2) AS avg_highway_mpg,
  ROUND(AVG(price), 2) AS avg_price,
  MAX(updated_date) AS updated_date,
  CURRENT_TIMESTAMP() AS created_date
FROM automotive_snapshot
WHERE city_mpg IS NOT NULL
  AND highway_mpg IS NOT NULL
GROUP BY fuel_type, body_style
{% endcall %}