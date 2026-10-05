#!/usr/bin/env bash

# shellcheck disable=SC1091
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

require_command uv
require_command terraform
require_command gcloud
require_command jq
[[ "${CONFIRM_GCS_UPLOAD:-no}" == "yes" ]] || die \
  "Set CONFIRM_GCS_UPLOAD=yes because this uploads Silver data and runs BigQuery load jobs."

: "${PROJECT_ID:?Set PROJECT_ID in scripts/metrics/metrics.env}"
BENCH_SHA="$(git rev-parse HEAD 2>/dev/null || printf 'uncommitted-workspace')"
BIGQUERY_LOCATION="${BIGQUERY_LOCATION:-US}"
EXPORT_DIR="${PROJECT_ROOT}/data/silver-export-${BENCH_SHA}"
SILVER_BUCKET="$(terraform -chdir=infra/terraform output -raw silver_bucket)"
DATASETS_JSON="$(terraform_output bigquery_datasets)"
GCS_ROOT="gs://${SILVER_BUCKET}/benchmark/${BENCH_SHA}"

[[ ! -e "${EXPORT_DIR}" ]] || die \
  "${EXPORT_DIR} already exists. Move it aside or use a new committed benchmark SHA."

log "Copying replayed Silver out of the Spark container"
"${COMPOSE[@]}" cp \
  "spark:/opt/atmospulse/data/silver-replay/${BENCH_SHA}" \
  "${EXPORT_DIR}"

log "Uploading Silver to ${GCS_ROOT}"
gcloud storage rsync --recursive "${EXPORT_DIR}" "${GCS_ROOT}"

export ATMOSPULSE_BQ_PROJECT="${PROJECT_ID}"
export ATMOSPULSE_BQ_LOCATION="${BIGQUERY_LOCATION}"
export ATMOSPULSE_BQ_STAGING_DATASET
export ATMOSPULSE_BQ_ANALYTICS_DATASET
export ATMOSPULSE_BQ_MONITORING_DATASET
export ATMOSPULSE_SILVER_ROOT="${GCS_ROOT}"
ATMOSPULSE_BQ_STAGING_DATASET="$(jq -r '.staging' <<<"${DATASETS_JSON}")"
ATMOSPULSE_BQ_ANALYTICS_DATASET="$(jq -r '.analytics' <<<"${DATASETS_JSON}")"
ATMOSPULSE_BQ_MONITORING_DATASET="$(jq -r '.monitoring' <<<"${DATASETS_JSON}")"

LOADED=0
for dataset in events posts engagements graph_edges identity quarantine; do
  if find "${EXPORT_DIR}/${dataset}" -name '*.parquet' -print -quit 2>/dev/null | grep -q .; then
    log "Loading ${dataset} into BigQuery"
    uv run python -m warehouse.loader \
      "${dataset}" \
      "${GCS_ROOT}/${dataset}/event_date=*/*.parquet"
    LOADED=$((LOADED + 1))
  else
    log "Skipping ${dataset}: the deterministic corpus produced no Parquet rows"
  fi
done

[[ "${LOADED}" -gt 0 ]] || die "No Silver Parquet datasets were found to load."
log "Cloud load complete (${LOADED} non-empty datasets)"
