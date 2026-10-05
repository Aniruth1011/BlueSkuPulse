#!/usr/bin/env bash

# shellcheck disable=SC1091
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

ACTION="${1:-plan}"
[[ "${ACTION}" == "plan" || "${ACTION}" == "apply" || "${ACTION}" == "outputs" ]] || die \
  "Usage: $0 [plan|apply|outputs]"
require_command terraform
require_command gcloud

: "${PROJECT_ID:?Set PROJECT_ID in scripts/metrics/metrics.env}"
: "${RESOURCE_PREFIX:?Set RESOURCE_PREFIX in scripts/metrics/metrics.env}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
GCP_REGION="${GCP_REGION:-us-west1}"
BIGQUERY_LOCATION="${BIGQUERY_LOCATION:-US}"
ALERT_EMAIL="${ALERT_EMAIL:-}"
PLAN_FILE="atmospulse-${ENVIRONMENT}.tfplan"

gcloud config set project "${PROJECT_ID}"
terraform -chdir=infra/terraform init

TF_ARGS=(
  -var="project_id=${PROJECT_ID}"
  -var="resource_prefix=${RESOURCE_PREFIX}"
  -var="environment=${ENVIRONMENT}"
  -var="region=${GCP_REGION}"
  -var="bigquery_location=${BIGQUERY_LOCATION}"
  -var="alert_email=${ALERT_EMAIL}"
)

case "${ACTION}" in
  plan)
    log "Creating a reviewable Terraform plan; this does not change GCP"
    terraform -chdir=infra/terraform fmt -check -recursive
    terraform -chdir=infra/terraform validate
    terraform -chdir=infra/terraform plan "${TF_ARGS[@]}" -out="${PLAN_FILE}"
    terraform -chdir=infra/terraform show "${PLAN_FILE}"
    ;;
  apply)
    [[ "${CONFIRM_TERRAFORM_APPLY:-no}" == "yes" ]] || die \
      "Set CONFIRM_TERRAFORM_APPLY=yes after reviewing the plan."
    [[ -f "infra/terraform/${PLAN_FILE}" ]] || die "Run the plan action first."
    log "Applying the previously reviewed Terraform plan"
    terraform -chdir=infra/terraform apply "${PLAN_FILE}"
    ;;
  outputs)
    terraform -chdir=infra/terraform output
    ;;
esac

