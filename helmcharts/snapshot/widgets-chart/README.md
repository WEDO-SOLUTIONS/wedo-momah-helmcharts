# widgets-chart

Helm chart for CityView Custom Widgets

## Install

```bash
# Install to staging
helm install widgets-chart ./widgets-chart -f widgets-chart/values.yaml -f widgets-chart/values/stg-values.yaml

# Install to production
helm install widgets-chart ./widgets-chart -f widgets-chart/values.yaml -f widgets-chart/values/prod-values.yaml
```

## Values

- `values.yaml` - default values
- `values/*-values.yaml` - environment-specific overrides (when present)

See [Chart.yaml](Chart.yaml) for version and app-version metadata.
