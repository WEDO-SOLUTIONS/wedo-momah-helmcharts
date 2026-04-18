# cityview-user-form-chart

Helm chart for User Form Service in CityView application

## Install

```bash
# Install to staging
helm install cityview-user-form-chart ./cityview-user-form-chart -f cityview-user-form-chart/values.yaml -f cityview-user-form-chart/values/stg-values.yaml

# Install to production
helm install cityview-user-form-chart ./cityview-user-form-chart -f cityview-user-form-chart/values.yaml -f cityview-user-form-chart/values/prod-values.yaml
```

## Values

- `values.yaml` - default values
- `values/*-values.yaml` - environment-specific overrides (when present)

See [Chart.yaml](Chart.yaml) for version and app-version metadata.
