{#
  Cross-dialect row generator for the Monte Carlo grid. DuckDB and Trino differ in how they
  generate rows, so each adapter produces `partitions * samples_per_partition` rows its own
  way and exposes a single `sample_id` column (0-based): DuckDB uses the `range()` table
  function; Trino cross-joins two bounded `sequence()` unnests (avoids building one giant
  array). Random x/y are drawn with `random()` (available on both engines).
#}
{% macro generate_samples(partitions, samples_per_partition) -%}
    {{ return(adapter.dispatch('generate_samples', 'pi')(partitions, samples_per_partition)) }}
{%- endmacro %} 

{% macro duckdb__generate_samples(partitions, samples_per_partition) -%}
    (
      SELECT (p.p * {{ samples_per_partition }} + s.s) AS sample_id
      FROM range(0, {{ partitions }}) AS p(p),
           range(0, {{ samples_per_partition }}) AS s(s)
    )
{%- endmacro %}

{% macro trino__generate_samples(partitions, samples_per_partition) -%}
    (
      SELECT (p * {{ samples_per_partition }} + s) AS sample_id
      FROM UNNEST(sequence(0, {{ partitions }} - 1)) AS t_p(p),
           UNNEST(sequence(0, {{ samples_per_partition }} - 1)) AS t_s(s)
    )
{%- endmacro %}
