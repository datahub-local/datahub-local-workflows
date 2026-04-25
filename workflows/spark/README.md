# spark

Helmfile state for deploying one or more `SparkApplication` custom resources.

## Structure

- `helmfile.yaml` declares the remote `spark-apps` chart release.
- `values.yaml` contains the base chart values and Spark application specs.
- `values.yaml.gotmpl` injects every file under `scripts/` into `scripts.files`.
- `scripts/` stores versioned PySpark scripts bundled into a ConfigMap.

## Usage

Update `values.yaml`, add or change files under `scripts/`, and render or deploy with Helmfile:

```bash
helmfile template
helmfile sync
```

Any file placed in `scripts/` is added automatically to `scripts.files` through the `getFiles` helper used by `values.yaml.gotmpl`.

## Argo CD

Argo CD does not render Helmfile natively. Use a config management plugin that runs `helmfile template`, or render manifests in CI and point Argo CD at the rendered output.

If you use a Helmfile plugin, point Argo CD at this path:

```yaml
spec:
  source:
    repoURL: <your-repository-url>
    targetRevision: main
    path: workflows/spark
    plugin:
      name: helmfile
```

If you need environment-specific configuration, add more values templates and reference them from `helmfile.yaml`.
