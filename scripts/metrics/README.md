# Metrics command runner

All long-running services start detached. One main terminal is sufficient. A second terminal is
optional for `./scripts/metrics/run.sh watch`.

Start with `validate`, then `smoke`. Commit the resulting code before `full`; the full command
refuses a dirty or uncommitted worktree so its JSON evidence can be tied to the exact Git SHA.

Cloud commands read `metrics.env`. Terraform apply and billed BigQuery execution require separate,
explicit confirmation environment variables; plan/smoke commands cannot accidentally trigger
those changes.

The normal sequence is `validate` -> `smoke` -> commit -> `full` -> `replay-failures`.
Cloud work then follows `gcp-plan` -> review -> `gcp-apply` -> `cloud-load` -> `bigquery`.
Use `archive-stop` last.

Confirm cloud mutations for one command only rather than saving confirmation in `metrics.env`:

```bash
CONFIRM_TERRAFORM_APPLY=yes ./scripts/metrics/run.sh gcp-apply
CONFIRM_GCS_UPLOAD=yes ./scripts/metrics/run.sh cloud-load
CONFIRM_BIGQUERY_COST=yes ./scripts/metrics/run.sh bigquery
```
