# middleware-chart

Helm chart for CityView Middleware

## Install

```bash
# Install to staging
helm install middleware-chart ./middleware-chart -f middleware-chart/values.yaml -f middleware-chart/values/stg-values.yaml

# Install to production
helm install middleware-chart ./middleware-chart -f middleware-chart/values.yaml -f middleware-chart/values/prod-values.yaml
```

## Values

- `values.yaml` - default values
- `values/*-values.yaml` - environment-specific overrides (when present)

See [Chart.yaml](Chart.yaml) for version and app-version metadata.
