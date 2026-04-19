# geoserver-cloud-chart

A Helm chart for Kubernetes to deploy GeoServer in a cloud environment.

## Install

```bash
# Install to staging
helm install geoserver-cloud-chart ./geoserver-cloud-chart -f geoserver-cloud-chart/values.yaml -f geoserver-cloud-chart/values/stg-values.yaml

# Install to production
helm install geoserver-cloud-chart ./geoserver-cloud-chart -f geoserver-cloud-chart/values.yaml -f geoserver-cloud-chart/values/prod-values.yaml
```

## Values

- `values.yaml` - default values
- `values/*-values.yaml` - environment-specific overrides (when present)

See [Chart.yaml](Chart.yaml) for version and app-version metadata.
