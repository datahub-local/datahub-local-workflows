# spark

Spark Declarative Pipeline project packaged with uv and deployed through Helmfile.

## Layout

- `src/core/` contains the core SparkApplication launcher code.
- `src/pipelines/` contains the pipeline project, including the declarative spec and SQL files.
- `tests/` validates the SQL pipeline files and runs an official `spark-pipelines dry-run` check.
- `tools/build_bundle.sh` builds the runtime bundle used by Spark.
- `deploy/` contains the Helmfile configuration that mounts the OCI bundle into Spark pods.

## Pipeline project

The project is intentionally pipeline-first now. The old helper package and bespoke Spark transformation code were removed so the maintained surface is the pipeline spec plus pipeline source files.

- `src/pipelines/{pipeline}/spark-pipeline.yml` is the declarative pipeline spec.
- `src/pipelines/sql/` contains the SQL files used by the pipeline spec.

These can be customized using Jinja templating to inject connection information from environment variables if file extensions match `.sql.j2`, so they can be rendered at runtime by Spark Declarative Pipelines.

The pipeline spec is `src/pipelines/{pipeline}/spark-pipeline.yml`. It defines The SQL files are in `src/pipelines/sql/` and use Jinja templating to inject connection information from environment variables.

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
