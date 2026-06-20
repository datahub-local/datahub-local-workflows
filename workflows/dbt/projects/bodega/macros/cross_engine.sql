{#
  Cross-dialect helpers for the bodega silver models. homelab runs on Trino, local on
  DuckDB; the two engines differ in timestamp parsing, JSON extraction, and how a JSON
  array is exploded with positional ordinality. Each helper dispatches on the adapter so
  the silver models share one body across both engines
  (same pattern as pi/macros/generate_samples.sql).
#}

{# Parse the ISO-8601 invoice timestamp string into a native timestamp. #}
{% macro bodega_parse_dt(col) -%}
    {{ return(adapter.dispatch('bodega_parse_dt', 'bodega')(col)) }}
{%- endmacro %}
{% macro trino__bodega_parse_dt(col) -%}
    date_parse({{ col }}, '%Y-%m-%dT%H:%i:%s')
{%- endmacro %}
{% macro duckdb__bodega_parse_dt(col) -%}
    strptime({{ col }}, '%Y-%m-%dT%H:%M:%S')
{%- endmacro %}

{# Extract a scalar string from a JSON element at the given path (e.g. '$.description'). #}
{% macro bodega_json_scalar(elem, path) -%}
    {{ return(adapter.dispatch('bodega_json_scalar', 'bodega')(elem, path)) }}
{%- endmacro %}
{% macro trino__bodega_json_scalar(elem, path) -%}
    json_extract_scalar({{ elem }}, '{{ path }}')
{%- endmacro %}
{% macro duckdb__bodega_json_scalar(elem, path) -%}
    json_extract_string({{ elem }}, '{{ path }}')
{%- endmacro %}

{# Number of elements in a JSON-array string column. #}
{% macro bodega_json_len(col) -%}
    {{ return(adapter.dispatch('bodega_json_len', 'bodega')(col)) }}
{%- endmacro %}
{% macro trino__bodega_json_len(col) -%}
    CARDINALITY(CAST(json_parse({{ col }}) AS ARRAY(JSON)))
{%- endmacro %}
{% macro duckdb__bodega_json_len(col) -%}
    json_array_length(CAST({{ col }} AS JSON))
{%- endmacro %}

{# SUM a numeric field (e.g. 'tax') across every element of a JSON-array string column. #}
{% macro bodega_json_sum(col, field) -%}
    {{ return(adapter.dispatch('bodega_json_sum', 'bodega')(col, field)) }}
{%- endmacro %}
{% macro trino__bodega_json_sum(col, field) -%}
    (
        SELECT SUM(CAST(json_extract_scalar(e, '$.{{ field }}') AS DOUBLE))
        FROM UNNEST(CAST(json_parse({{ col }}) AS ARRAY(JSON))) AS u(e)
    )
{%- endmacro %}
{% macro duckdb__bodega_json_sum(col, field) -%}
    (
        SELECT SUM(CAST(json_extract_string(e, '$.{{ field }}') AS DOUBLE))
        FROM UNNEST(json_extract({{ col }}, '$[*]')) AS u(e)
    )
{%- endmacro %}

{# FROM-clause fragment that explodes a JSON-array string column into one row per element,
   exposing `_it.elem` (the JSON element) and `_it.pos` (1-based position). #}
{% macro bodega_explode_json(col) -%}
    {{ return(adapter.dispatch('bodega_explode_json', 'bodega')(col)) }}
{%- endmacro %}
{% macro trino__bodega_explode_json(col) -%}
    CROSS JOIN UNNEST(CAST(json_parse({{ col }}) AS ARRAY(JSON))) WITH ORDINALITY AS _it(elem, pos)
{%- endmacro %}
{% macro duckdb__bodega_explode_json(col) -%}
    CROSS JOIN (
        SELECT json_extract({{ col }}, '$[' || g.i || ']') AS elem, g.i + 1 AS pos
        FROM range(CAST(json_array_length(CAST({{ col }} AS JSON)) AS BIGINT)) AS g(i)
    ) AS _it
{%- endmacro %}
