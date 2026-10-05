#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMMAND="${1:-help}"
shift || true

case "${COMMAND}" in
  validate) exec "${SCRIPT_DIR}/01_validate.sh" "$@" ;;
  smoke) exec "${SCRIPT_DIR}/02_local_benchmark.sh" smoke ;;
  full) exec "${SCRIPT_DIR}/02_local_benchmark.sh" full ;;
  replay-failures) exec "${SCRIPT_DIR}/03_replay_and_failures.sh" "$@" ;;
  gcp-plan) exec "${SCRIPT_DIR}/04_gcp_infrastructure.sh" plan ;;
  gcp-apply) exec "${SCRIPT_DIR}/04_gcp_infrastructure.sh" apply ;;
  gcp-outputs) exec "${SCRIPT_DIR}/04_gcp_infrastructure.sh" outputs ;;
  cloud-load) exec "${SCRIPT_DIR}/05_cloud_load.sh" ;;
  bigquery) exec "${SCRIPT_DIR}/05_bigquery_benchmark.sh" ;;
  archive-stop) exec "${SCRIPT_DIR}/06_archive_and_stop.sh" ;;
  watch) exec "${SCRIPT_DIR}/watch.sh" ;;
  help|*)
    cat <<'EOF'
Usage: ./scripts/metrics/run.sh COMMAND

Commands:
  validate          Run all local code, Spark, and dbt validation
  smoke             Run a quick 10,000-event Kafka validation
  full              Run sustained, burst, 5M replay, and duplicate benchmarks
  replay-failures   Rebuild Silver and run controlled failure drills
  gcp-plan          Create and show a Terraform plan
  gcp-apply         Apply the reviewed plan (requires explicit confirmation)
  gcp-outputs       Show deployed infrastructure outputs
  cloud-load        Upload replayed Silver and load non-empty BigQuery tables
  bigquery          Run billed naive-vs-optimized comparison (explicit confirmation)
  archive-stop      Archive evidence and stop local services
  watch             Follow pipeline logs in an optional second terminal
EOF
    ;;
esac
