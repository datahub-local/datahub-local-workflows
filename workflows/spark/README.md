# spark

Spark Declarative Pipeline project packaged with uv and a PEX runtime, then deployed through Helmfile.

The exact Spark release is pinned in `.spark-version` and reused by Python packaging metadata, the runtime bundle build, and Helm deployment values.

## Layout

- `src/core/` contains the core SparkApplication launcher code.
- `src/pipelines/` contains the pipeline project, including the declarative spec and SQL files.
- `tests/` validates the SQL pipeline files and runs an official `spark-pipelines dry-run` check.
- `Dockerfile.bundle` builds the OCI runtime bundle used by Spark.
- `deploy/` contains the Helmfile configuration that mounts the OCI bundle into Spark pods.

## Pipeline project

The project is intentionally pipeline-first now. The old helper package and bespoke Spark transformation code were removed so the maintained surface is the pipeline spec plus pipeline source files.

- `src/pipelines/{pipeline}/spark-pipeline.yml` is the declarative pipeline spec.
- `src/pipelines/sql/` contains the SQL files used by the pipeline spec.

These can be customized using Jinja templating to inject connection information from environment variables if file extensions match `.sql.j2`, so they can be rendered at runtime by Spark Declarative Pipelines.

The pipeline spec is `src/pipelines/{pipeline}/spark-pipeline.yml`. It defines The SQL files are in `src/pipelines/sql/` and use Jinja templating to inject connection information from environment variables.

For local development:

```bash
uv python install
uv sync --group dev
uv run pytest
```

## Runtime bundle

Build the runtime bundle image locally with:

```bash
docker buildx build \
	--platform linux/amd64 \
	-f Dockerfile.bundle \
	--build-arg PYTHON_VERSION="$(tr -d '\n' < .python-version)" \
	--build-arg SPARK_VERSION="$(tr -d '\n' < .spark-version)" \
	--output type=oci,dest=/tmp/datahub-local-workflows-spark-bundle.oci \
	.
```

The resulting OCI bundle image contains:

- `/deps/pyspark_pex_env.pex` with the non-dev Python dependencies needed by Spark Declarative Pipelines.
- `/src/core/launchers/spark_pipelines.py` as the Spark entrypoint mounted into the pod.
- `/src/pipelines/` as the pipeline project files.

The deployed Spark application points `spark.pyspark.python` at the mounted PEX so driver and executor Python processes resolve the same dependency set.

`Dockerfile.bundle` exports the locked runtime requirements from project metadata then rebuilds `pyspark_pex_env.pex` inside `apache/spark:${SPARK_VERSION}` so dependency wheels are resolved for the same Spark runtime image family used in the cluster.

The GitHub workflow in `.github/workflows/publish-spark-bundle.yaml` publishes a multi-architecture bundle image for `linux/amd64` and `linux/arm64` as `ghcr.io/datahub-local/datahub-local-workflows-spark-bundle:main`.

## Validation

The test suite now validates the pipeline project directly:

- it executes the SQL pipeline definitions in a local Spark session with test configuration values.
- it runs `spark-pipelines dry-run --spec src/pipelines/spark-pipeline.yml` against a temporary project copy.

## Deployment

Deployment configuration moved to `deploy/`. See `deploy/README.md` for Helmfile usage and cluster requirements.
