# ump-ui-chart

Helm chart for User Management Portal UI

## Install

```bash
# Install to staging
helm install ump-ui-chart ./ump-ui-chart -f ump-ui-chart/values.yaml -f ump-ui-chart/values/stg-values.yaml

# Install to production
helm install ump-ui-chart ./ump-ui-chart -f ump-ui-chart/values.yaml -f ump-ui-chart/values/prod-values.yaml
```

## Values

- `values.yaml` - default values
- `values/*-values.yaml` - environment-specific overrides (when present)

See [Chart.yaml](Chart.yaml) for version and app-version metadata.
