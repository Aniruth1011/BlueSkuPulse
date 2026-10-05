# AtmosPulse Terraform

This root provisions the durable GCP boundary: immutable Bronze and replaceable Silver buckets,
BigQuery datasets, stable monitoring tables, workload service accounts, least-privilege IAM,
empty Secret Manager containers, and a cloud-resource dashboard. It deliberately provisions no
always-on compute; local Docker and managed, bounded jobs remain the cheaper default.

## Ownership decision

- Terraform owns buckets, datasets, IAM, service accounts, secret containers, monitoring
  resources, and stable operational tables.
- `warehouse.loader` owns schema-evolving `silver_*` landing tables. Giving those tables to both
  Terraform and the loader would cause schema drift fights.
- dbt owns dimensions, facts, and marts inside the analytics dataset.
- Secret *values* are populated by an approved secret-delivery process, never Terraform, so they
  do not enter state.

## Validate and plan

```bash
terraform -chdir=infra/terraform fmt -check -recursive
terraform -chdir=infra/terraform init -backend=false
terraform -chdir=infra/terraform validate
terraform -chdir=infra/terraform plan -var-file=terraform.tfvars
```

Copy `terraform.tfvars.example` locally and choose a globally unique `resource_prefix`. Production
has deletion protection regardless of `force_destroy_nonprod`. A checked-in backend is omitted on
purpose; `backend.tf.example` documents the explicit remote-state handoff.

