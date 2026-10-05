#!/usr/bin/env bash

# shellcheck disable=SC1091
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

MODE="${1:-smoke}"
[[ "${MODE}" == "smoke" || "${MODE}" == "full" ]] || die "Usage: $0 [smoke|full]"
require_command uv
require_command docker
require_command git
configure_java

if [[ "${MODE}" == "full" ]]; then
  require_clean_commit
else
  BENCH_SHA="$(git rev-parse HEAD 2>/dev/null || printf 'uncommitted-workspace')"
fi

if ! docker image inspect atmospulse-bronze:local >/dev/null 2>&1; then
  log "Building the missing Bronze image"
  docker build --file docker/app.Dockerfile --target bronze --tag atmospulse-bronze:local .
fi
if ! docker image inspect atmospulse-spark:local >/dev/null 2>&1; then
  log "Building the missing Spark image"
  docker build --file docker/app.Dockerfile --target spark --tag atmospulse-spark:local .
fi

log "Starting Redpanda, Bronze, and Spark in the background"
"${COMPOSE[@]}" --profile pipeline up -d --no-build redpanda topic-init bronze spark

if [[ "${MODE}" == "smoke" ]]; then
  log "Running a 10,000-event Kafka validation"
  uv run python -m benchmarking.run smoke \
    --transport kafka \
    --bootstrap-servers localhost:19092
else
  log "Running sustained workload: 1,000 events/s for 30 minutes"
  uv run python -m benchmarking.run sustained \
    --transport kafka \
    --bootstrap-servers localhost:19092

  log "Running burst workload: 5,000 events/s for 5 minutes"
  uv run python -m benchmarking.run burst \
    --transport kafka \
    --bootstrap-servers localhost:19092

  log "Publishing the five-million-event replay corpus"
  uv run python -m benchmarking.run replay \
    --transport kafka \
    --bootstrap-servers localhost:19092

  log "Running ten-percent duplicate delivery"
  uv run python -m benchmarking.run duplicates \
    --transport kafka \
    --bootstrap-servers localhost:19092
fi

log "Capturing container resource usage"
docker stats --no-stream > "benchmark/results/docker-stats-${MODE}-${BENCH_SHA}.txt"

log "Current Bronze consumer position"
"${COMPOSE[@]}" exec -T redpanda \
  rpk group describe atmospulse-bronze-v1 -X brokers=redpanda:9092 || true

log "Benchmark complete. Results are under benchmark/results/"
