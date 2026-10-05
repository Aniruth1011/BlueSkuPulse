# ADR 0006: Keep analytics, orchestration, and observability ownership separate

Status: accepted

## Context

AtmosPulse needs useful analytical products, repeatable scheduled workflows, and operational
visibility. Putting SQL in dashboards, running the WebSocket from Airflow, or treating logs as a
monitoring system would blur ownership and make replay behavior difficult to prove.

## Decision

- dbt owns analytical definitions and produces query-ready BigQuery marts. Grafana is a thin
  consumer of those marts.
- Airflow owns bounded workflows only. Its tasks use explicit data intervals and call ordinary
  CLI modules that are testable without the scheduler.
- The ingestion WebSocket and Spark streaming queries remain independently supervised services.
- Long-running services expose pull-based Prometheus metrics. Short-lived dbt and reconciliation
  jobs use Pushgateway because they are service-level batch jobs.
- Prometheus owns alert evaluation; Grafana owns visualization; Alertmanager owns routing.

## Trade-offs

- Recursive conversation reconstruction is more expensive than a shallow reply count, but it
  produces defensible depth and branching measures. Recursion is capped at 100 and unresolved
  parent chains are counted as orphans instead of discarded.
- Topic windows are computed for 5, 15, and 60 minutes in one mart. This scans more rows than a
  single window but prevents three definitions from drifting apart.
- Hourly warehouse loads may present the same daily Silver partition repeatedly because Silver is
  date-partitioned. Deterministic `event_id` merges keep this correct; a future measured cost study
  may justify an hour partition or manifest-driven URI list.
- Grafana uses the official BigQuery plugin with workload identity/GCE authentication in cloud.
  No service-account key is stored in the repository. Local business panels therefore require the
  operator to supply valid Google credentials.
- The Airflow image isolates dbt/Spark dependencies in a separate virtual environment. The image
  is larger, but scheduler dependencies cannot be destabilized by analytical packages.

## Consequences

The same bounded commands can be run manually, by Airflow, or by CI. Retrying an interval does not
change canonical results. Operational alerts do not depend on dashboard SQL, and dashboard logic
does not become an untested second transformation layer.
