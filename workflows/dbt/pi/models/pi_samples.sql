SELECT
  sample_id,
  partitions,
  samples_per_partition,
  random_seed,
  x,
  y,
  CASE WHEN (x * x) + (y * y) <= 1.0 THEN 1 ELSE 0 END AS is_hit
FROM (
  SELECT
    sample_id,
    CAST({{ var('partitions') }} AS INTEGER)                AS partitions,
    CAST({{ var('samples_per_partition') }} AS INTEGER)     AS samples_per_partition,
    CAST({{ var('random_seed') }} AS INTEGER)               AS random_seed,
    (random() * 2.0) - 1.0                                  AS x,
    (random() * 2.0) - 1.0                                  AS y
  FROM {{ generate_samples(var('partitions'), var('samples_per_partition')) }} AS g
)
