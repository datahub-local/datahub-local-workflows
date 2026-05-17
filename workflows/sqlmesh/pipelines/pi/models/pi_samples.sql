MODEL (
  name pi.pi_samples,
  kind FULL,
  dialect spark,
  description 'Monte Carlo sample generation for pi estimation. Uses project variables partitions, samples_per_partition, and random_seed.'
);

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
    id                                                           AS sample_id,
    CAST(@partitions AS INT)                                     AS partitions,
    CAST(@samples_per_partition AS INT)                          AS samples_per_partition,
    CAST(@random_seed AS INT)                                    AS random_seed,
    (RAND(CAST(@random_seed AS INT)) * 2.0) - 1.0               AS x,
    (RAND(CAST(@random_seed AS INT) + 1) * 2.0) - 1.0           AS y
  FROM RANGE(
    0,
    CAST(@partitions AS BIGINT) * CAST(@samples_per_partition AS BIGINT),
    1,
    CAST(@partitions AS INT)
  )
)
