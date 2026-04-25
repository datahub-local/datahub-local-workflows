# spark

Helm chart for deploying one or more `SparkApplication` custom resources.

## Structure

- `Chart.yaml` defines the chart metadata.
- `values.yaml` contains one or more Spark application specs under `sparkApplications`.
- `scripts/` stores versioned PySpark scripts that can be bundled into a ConfigMap.

## Usage

Update `values.yaml` or provide a separate values file and enable the applications you want Argo CD to deploy.

## Argo CD

Point an Argo CD `Application` at this chart path:

```yaml
spec:
  source:
    repoURL: <your-repository-url>
    targetRevision: main
    path: workflows/spark
```

If you need environment-specific configuration, keep separate values files and reference them from Argo CD.

Example using Git integration with Helm values from this repository:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: spark
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/datahub-local/datahub-local-workflows.git
    targetRevision: main
    path: workflows/spark
    helm:
      valueFiles:
        - values.yaml
  destination:
    server: https://kubernetes.default.svc
    namespace: data
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```
