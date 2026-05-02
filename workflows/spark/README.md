# spark

Spark Declarative Pipeline project packaged with uv and deployed through Helmfile.

## Layout

- `src/pipelines/spark-pipeline.yml` is the pipeline spec described in the Spark Declarative Pipelines guide.
- `src/pipelines/sql/*.sql` contains the declarative dataset definitions.
- `src/launchers/spark_pipelines.py` is the thin SparkApplication bridge that invokes the official `spark-pipelines` runtime.
- `tests/` validates the SQL pipeline files and runs an official `spark-pipelines dry-run` check.
- `tools/build_bundle.sh` builds the runtime bundle used by Spark.
- `deploy/` contains the Helmfile configuration that mounts the OCI bundle into Spark pods.

## Pipeline project

The project is intentionally pipeline-first now. The old helper package and bespoke Spark transformation code were removed so the maintained surface is the pipeline spec plus pipeline source files.

The pipeline currently uses SQL files to define:

- `pi_samples` as a temporary view of sampled points.
- `pi_estimate` as a materialized view of the aggregated Pi estimate.

Default pipeline tuning lives in `src/pipelines/spark-pipeline.yml` and can be overridden with Spark configuration:

- `spark.datahub_local_workflows.pipeline.partitions`
- `spark.datahub_local_workflows.pipeline.samples_per_partition`
- `spark.datahub_local_workflows.pipeline.random_seed`

For local development:

```bash
uv python install 3.11
uv sync --python 3.11 --group dev
uv run --python 3.11 pytest
```

## Runtime bundle

Build the runtime bundle locally with:

```bash
sh tools/build_bundle.sh 3.11
```

This produces:

- `build/runtime/python-deps.zip` with the non-dev Python dependencies needed by Spark Declarative Pipelines.
- `build/runtime/src/launchers/spark_pipelines.py` as the Spark entrypoint mounted into the pod.
- `build/runtime/src/pipelines/` as the pipeline project files.

The GitHub workflow in `.github/workflows/publish-spark-bundle.yaml` publishes these files as `ghcr.io/datahub-local/datahub-local-workflows-spark-bundle:main`.

## Validation

The test suite now validates the pipeline project directly:

- it executes the SQL pipeline definitions in a local Spark session with test configuration values.
- it runs `spark-pipelines dry-run --spec src/pipelines/spark-pipeline.yml` against a temporary project copy.

## Deployment

Deployment configuration moved to `deploy/`. See `deploy/README.md` for Helmfile usage and cluster requirements.
