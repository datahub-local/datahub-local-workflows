CREATE TEMPORARY VIEW pi_samples AS
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
    id AS sample_id,
    CAST(${spark.pipeline.partitions} AS INT) AS partitions,
    CAST(${spark.pipeline.samples_per_partition} AS INT) AS samples_per_partition,
    CAST(${spark.pipeline.random_seed} AS INT) AS random_seed,
    (RAND(CAST(${spark.pipeline.random_seed} AS INT)) * 2.0) - 1.0 AS x,
    (RAND(CAST(${spark.pipeline.random_seed} AS INT) + 1) * 2.0) - 1.0 AS y
  FROM RANGE(
    0,
    CAST(${spark.pipeline.partitions} AS BIGINT) * CAST(${spark.pipeline.samples_per_partition} AS BIGINT),
    1,
    CAST(${spark.pipeline.partitions} AS INT)
  )
);