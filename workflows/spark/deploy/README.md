# spark deploy

Helmfile state for deploying one or more `SparkApplication` custom resources from the published Spark declarative pipeline bundle.

## Layout

- `helmfile.yaml.gotmpl` declares the remote `spark-apps` chart release.
- `values.yaml.gotmpl` configures Spark applications, including the mounted OCI runtime bundle.

## Runtime model

The Spark application no longer mounts scripts from a ConfigMap. Instead, driver and executor pods mount the published OCI image volume at `/opt/spark/app`.

The mounted image provides:

- `pyspark_pex_env.pex` as the Python executable selected by `spark.pyspark.python`
- `src/core/*` as the core SparkApplication launcher code
- `src/pipelines/**` as the pipeline  loaded by Spark Declarative Pipelines

The published bundle image is multi-architecture (`linux/amd64` and `linux/arm64`). `Dockerfile.bundle` rebuilds the PEX directly from `pyproject.toml` during the OCI image build so native wheels match the target architecture.

The chart reads the Spark runtime version from `../.spark-version`, so the Helm values stay aligned with the pinned `pyspark` dependency and the bundle build.

The SparkApplication passes pipeline tuning through Spark configuration overrides:

- `spark.datahub_local_workflows.pipeline.partitions`
- `spark.datahub_local_workflows.pipeline.samples_per_partition`
- `spark.datahub_local_workflows.pipeline.random_seed`

The default bundle reference is:

```text
ghcr.io/datahub-local/datahub-local-workflows-spark-bundle:main
```

## Cluster requirement

This deployment uses Kubernetes `image` volumes. That requires a cluster and runtime that support image volumes.

## Usage

Render or deploy from this directory:

```bash
helmfile template
helmfile sync
```

## Argo CD

Argo CD does not render Helmfile natively. Use a config management plugin that runs `helmfile template`, or render manifests in CI and point Argo CD at the rendered output.

If you use a Helmfile plugin, point Argo CD at this path:

```yaml
spec:
  source:
    repoURL: <your-repository-url>
    targetRevision: main
    path: workflows/spark/deploy
    plugin:
      name: helmfile
```