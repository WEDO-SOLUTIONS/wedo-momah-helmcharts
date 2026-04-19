# permissions-api-chart

Helm chart for CityView Custom Permissions API

## Install

```bash
# Install to staging
helm install permissions-api-chart ./permissions-api-chart -f permissions-api-chart/values.yaml -f permissions-api-chart/values/stg-values.yaml

# Install to production
helm install permissions-api-chart ./permissions-api-chart -f permissions-api-chart/values.yaml -f permissions-api-chart/values/prod-values.yaml
```

## Values

- `values.yaml` - default values
- `values/*-values.yaml` - environment-specific overrides (when present)

See [Chart.yaml](Chart.yaml) for version and app-version metadata.
