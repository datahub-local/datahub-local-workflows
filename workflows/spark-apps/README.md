# spark-apps

Helm chart for deploying one or more `SparkApplication` custom resources with Argo CD.

## Structure

- `Chart.yaml` defines the chart metadata.
- `values.yaml` contains one or more Spark application specs under `sparkApplications`.
- `scripts/` stores versioned PySpark scripts that can be bundled into a ConfigMap.
- `examples/` contains example values and an Argo CD application manifest.
- `templates/sparkapplication.yaml` renders a `SparkApplication` per enabled entry.
- `templates/app-resources.yaml` renders shared ConfigMaps and Secrets from values.

## Usage

Update `values.yaml` or provide a separate values file and enable the applications you want Argo CD to deploy.

Example values:

```yaml
imagePullSecrets:
  - regcred

serviceAccount:
  create: false
  name: spark

scripts:
  enabled: true
  mountPath: /opt/spark/scripts

sharedConfigMaps:
  - enabled: true
    id: common-env
    name: spark-shared-config
    envFrom: true
    mountPath: /opt/spark/conf/app
    data:
      APP_ENV: dev
      LOG_LEVEL: INFO

sharedSecrets:
  - enabled: true
    id: api-token
    name: spark-shared-secret
    envFrom: true
    stringData:
      API_TOKEN: change-me
  - enabled: true
    id: tls-material
    name: spark-tls-secret
    mountPath: /opt/spark/certs
    stringData:
      ca.crt: change-me

sparkApplications:
  - name: sample-pi
    enabled: true
    type: Python
    mode: cluster
    sparkVersion: "3.5.1"
    image: ghcr.io/example/spark:3.5.1
    mainApplicationFile: local:///opt/spark/scripts/example_job.py
    arguments:
      - "--message"
      - "hello from argo"
    restartPolicy:
      type: Never
    sharedConfigMapRefs:
      - common-env
    sharedSecretRefs:
      - api-token
    driver:
      cores: 1
      coreLimit: 1200m
      memory: 512m
    executor:
      instances: 1
      cores: 1
      memory: 512m
```

When `scripts.enabled` is true, the chart packages everything under `scripts/` into a ConfigMap and mounts it into both the driver and executor containers.

`imagePullSecrets` are applied to every rendered SparkApplication and should be provided as a list of secret names.

`serviceAccount` defines the chart-level driver service account. Set `serviceAccount.create: true` to render the ServiceAccount resource and override it per app with `driver.serviceAccount` when needed.

    Shared `sharedConfigMaps` and `sharedSecrets` entries are rendered once per Helm release and can do both of the following:

    - create Kubernetes ConfigMaps and Secrets from values
    - automatically append them to every SparkApplication as `envFrom` references and read-only mounted volumes

Each SparkApplication can restrict which shared resources are attached by setting `sharedConfigMapRefs` and `sharedSecretRefs`. If those lists are empty, all enabled shared resources are attached.

    Each shared resource entry supports these useful fields:

    - `enabled`
- `id`
    - `name`
    - `envFrom`
    - `mountPath`
    - `attachToDriver`
    - `attachToExecutor`
    - `items`
    - `optional`
    - `data`, `binaryData`, or `stringData`

## Argo CD

Point an Argo CD `Application` at this chart path:

```yaml
spec:
  source:
    repoURL: <your-repository-url>
    targetRevision: main
    path: workflows/spark-apps
```

If you need environment-specific configuration, keep separate values files and reference them from Argo CD.

Example using Git integration with Helm values from this repository:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: spark-apps
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/datahub-local/datahub-local-workflows.git
    targetRevision: main
    path: workflows/spark-apps
    helm:
      valueFiles:
        - examples/values-example.yaml
  destination:
    server: https://kubernetes.default.svc
    namespace: data
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```

    ## Testing

    Run Helm unit tests with:

    ```bash
    helm unittest workflows/spark-apps
    ```

    See [examples/values-example.yaml](examples/values-example.yaml), [examples/values-production.yaml](examples/values-production.yaml), and [examples/argocd-application.yaml](examples/argocd-application.yaml) for working starting points.


