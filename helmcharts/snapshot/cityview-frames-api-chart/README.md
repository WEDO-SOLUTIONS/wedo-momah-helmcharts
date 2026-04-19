# cityview-frames-api-chart

Helm chart for CityView Frames API

## Install

```bash
# Install to staging
helm install cityview-frames-api-chart ./cityview-frames-api-chart -f cityview-frames-api-chart/values.yaml -f cityview-frames-api-chart/values/stg-values.yaml

# Install to production
helm install cityview-frames-api-chart ./cityview-frames-api-chart -f cityview-frames-api-chart/values.yaml -f cityview-frames-api-chart/values/prod-values.yaml
```

## Values

- `values.yaml` - default values
- `values/*-values.yaml` - environment-specific overrides (when present)

See [Chart.yaml](Chart.yaml) for version and app-version metadata.
