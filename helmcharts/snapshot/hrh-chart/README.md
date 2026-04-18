# hrh-chart

Helm chart for CityView HRH UI

## Install

```bash
# Install to staging
helm install hrh-chart ./hrh-chart -f hrh-chart/values.yaml -f hrh-chart/values/stg-values.yaml

# Install to production
helm install hrh-chart ./hrh-chart -f hrh-chart/values.yaml -f hrh-chart/values/prod-values.yaml
```

## Values

- `values.yaml` - default values
- `values/*-values.yaml` - environment-specific overrides (when present)

See [Chart.yaml](Chart.yaml) for version and app-version metadata.
