# cityview-activity-log-chart

Helm chart for CityView Activity Log

## Install

```bash
# Install to staging
helm install cityview-activity-log-chart ./cityview-activity-log-chart -f cityview-activity-log-chart/values.yaml -f cityview-activity-log-chart/values/stg-values.yaml

# Install to production
helm install cityview-activity-log-chart ./cityview-activity-log-chart -f cityview-activity-log-chart/values.yaml -f cityview-activity-log-chart/values/prod-values.yaml
```

## Values

- `values.yaml` - default values
- `values/*-values.yaml` - environment-specific overrides (when present)

See [Chart.yaml](Chart.yaml) for version and app-version metadata.
