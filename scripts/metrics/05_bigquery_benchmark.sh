#!/usr/bin/env bash

# shellcheck disable=SC1091
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

require_command uv
require_command terraform
require_command bq
require_command jq
[[ "${CONFIRM_BIGQUERY_COST:-no}" == "yes" ]] || die \
  "Set CONFIRM_BIGQUERY_COST=yes because this script creates tables and runs billed queries."

: "${PROJECT_ID:?Set PROJECT_ID in scripts/metrics/metrics.env}"
BIGQUERY_LOCATION="${BIGQUERY_LOCATION:-US}"
START_DATE="${START_DATE:-2026-01-01}"
END_DATE="${END_DATE:-2026-01-02}"
STAGING_DATASET="$(terraform_output bigquery_datasets | jq -r '.staging')"
BENCHMARK_DATASET="${BENCHMARK_DATASET:-atmospulse_benchmark}"
SOURCE_TABLE="${PROJECT_ID}.${STAGING_DATASET}.silver_events"
NAIVE_TABLE="${PROJECT_ID}.${BENCHMARK_DATASET}.naive_events"
OPTIMIZED_TABLE="${PROJECT_ID}.${BENCHMARK_DATASET}.optimized_events"

bq show "${PROJECT_ID}:${STAGING_DATASET}.silver_events" >/dev/null 2>&1 || die \
  "${SOURCE_TABLE} does not exist. Run: ./scripts/metrics/run.sh cloud-load"

log "Creating the dedicated BigQuery benchmark dataset when absent"
bq --location="${BIGQUERY_LOCATION}" mk --dataset \
  "${PROJECT_ID}:${BENCHMARK_DATASET}" 2>/dev/null || true

log "Creating equal-data naive and optimized benchmark tables"
bq query --location="${BIGQUERY_LOCATION}" --use_legacy_sql=false "
CREATE OR REPLACE TABLE \`${NAIVE_TABLE}\` AS
SELECT * FROM \`${SOURCE_TABLE}\`
"
bq query --location="${BIGQUERY_LOCATION}" --use_legacy_sql=false "
CREATE OR REPLACE TABLE \`${OPTIMIZED_TABLE}\`
PARTITION BY event_date
CLUSTER BY collection, did, operation AS
SELECT * FROM \`${SOURCE_TABLE}\`
"

log "Running a dry-run scan comparison"
uv run python -m benchmarking.bigquery_layout \
  --project "${PROJECT_ID}" \
  --location "${BIGQUERY_LOCATION}" \
  --naive-table "${NAIVE_TABLE}" \
  --optimized-table "${OPTIMIZED_TABLE}" \
  --start-date "${START_DATE}" \
  --end-date "${END_DATE}"

log "Running the billed runtime comparison"
uv run python -m benchmarking.bigquery_layout \
  --project "${PROJECT_ID}" \
  --location "${BIGQUERY_LOCATION}" \
  --naive-table "${NAIVE_TABLE}" \
  --optimized-table "${OPTIMIZED_TABLE}" \
  --start-date "${START_DATE}" \
  --end-date "${END_DATE}" \
  --execute
