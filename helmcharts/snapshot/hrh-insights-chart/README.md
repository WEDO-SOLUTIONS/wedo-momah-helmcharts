# hrh-insights-chart

Helm chart for HRH Insights backend

## Install

```bash
# Install to staging
helm install hrh-insights-chart ./hrh-insights-chart -f hrh-insights-chart/values.yaml -f hrh-insights-chart/values/stg-values.yaml

# Install to production
helm install hrh-insights-chart ./hrh-insights-chart -f hrh-insights-chart/values.yaml -f hrh-insights-chart/values/prod-values.yaml
```

## Values

- `values.yaml` - default values
- `values/*-values.yaml` - environment-specific overrides (when present)

See [Chart.yaml](Chart.yaml) for version and app-version metadata.
