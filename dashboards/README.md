# Analytics dashboard

The deployable business dashboard is version-controlled at
`monitoring/grafana/dashboards/analytics-products.json`. Its panels are thin consumers of dbt marts
for network activity, topics, domains, creators, post engagement, and conversations.

The BigQuery data source is provisioned without a key and expects workload identity or a GCE
service account. This repository intentionally does not contain dashboard credentials.
