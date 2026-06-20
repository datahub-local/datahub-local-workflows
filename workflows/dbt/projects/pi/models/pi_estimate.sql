SELECT
  COUNT(*)                           AS total_samples,
  CAST(SUM(is_hit) AS BIGINT)        AS hit_count,
  AVG(CAST(is_hit AS DOUBLE)) * 4.0  AS pi_estimate,
  MAX(partitions)                    AS partitions,
  MAX(samples_per_partition)         AS samples_per_partition,
  MAX(random_seed)                   AS random_seed
FROM {{ ref('pi_samples') }}
