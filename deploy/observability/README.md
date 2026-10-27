# Observability assets

- `grafana/felix-dashboard.json`: import into Grafana; it asks for a Prometheus data source.
- `prometheus/felix-alerts.yaml`: list under `rule_files` in Prometheus. The Helm chart renders the
  same rules as a `PrometheusRule` with `prometheusRule.enabled=true`, from its own copy in
  `deploy/helm/felix/files/`. Edit this one and copy it over; `scripts/check_observability_assets.py`
  fails if the two differ or if either file queries a metric the code does not register.

What each panel and alert means: [Observability](https://docs.getfelix.dev/features/observability/#dashboard-and-alert-rules).
