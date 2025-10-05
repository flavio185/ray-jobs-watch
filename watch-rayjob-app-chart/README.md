
---

### Step 2: Create the Helm Chart Structure

Helm uses a standard directory structure. Create these files and directories:

```
watch-rayjob-app-chart/
├── Chart.yaml
├── values.yaml
└── templates/
    ├── _helpers.tpl
    ├── configmap.yaml
    ├── deployment.yaml
    ├── pvc.yaml
    └── service.yaml

```
helm lint ./watch-rayjob-app-chart

# Choose a namespace name, e.g., 'project-alpha'
export NAMESPACE="app-ns"

# Create the namespace if it doesn't exist
kubectl create namespace $NAMESPACE

# Install the chart into the namespace
# helm install [RELEASE_NAME] [CHART_PATH] --namespace [NAMESPACE]
helm install fastapi-app ./watch-rayjob-app-chart --namespace $NAMESPACE

If you needed to override a value (like the image repository), you would use the `--set` flag:
```bash
helm install fastapi-app ./watch-rayjob-app-chart \
  --namespace $NAMESPACE \
  --set image.repository=my-docker-hub/my-cool-repo