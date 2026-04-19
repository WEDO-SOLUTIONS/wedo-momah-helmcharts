# ump-api-chart

Helm chart for User Management Portal API

## Install

```bash
# Install to staging
helm install ump-api-chart ./ump-api-chart -f ump-api-chart/values.yaml -f ump-api-chart/values/stg-values.yaml

# Install to production
helm install ump-api-chart ./ump-api-chart -f ump-api-chart/values.yaml -f ump-api-chart/values/prod-values.yaml
```

## Values

- `values.yaml` - default values
- `values/*-values.yaml` - environment-specific overrides (when present)

See [Chart.yaml](Chart.yaml) for version and app-version metadata.
