{% macro jdbc_table(
    spark_table_name='source_table',
    db_url='jdbc:h2:mem:testdb',
    schema_name='public',
    table_name='test',
    db_user='user',
    db_password='',
    driver='org.h2.Driver'
) -%}
CREATE OR REPLACE TABLE {{ spark_table_name }}
USING jdbc
OPTIONS (
  url '{{ db_url }}',
  dbtable '{{ schema_name }}.{{ table_name }}',
  user '{{ db_user }}',
  password '{{ db_password }}',
  driver '{{ driver }}'
)
AS
{{ caller() | trim }}
;
{%- endmacro %}

