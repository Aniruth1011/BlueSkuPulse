#!/usr/bin/env bash

# shellcheck disable=SC1091
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

require_command uv
require_command docker
configure_java
BENCH_SHA="$(git rev-parse HEAD 2>/dev/null || printf 'uncommitted-workspace')"
BRONZE_DRAIN_SECONDS="${BRONZE_DRAIN_SECONDS:-60}"

log "Waiting ${BRONZE_DRAIN_SECONDS}s for Bronze to drain; increase BRONZE_DRAIN_SECONDS for large runs"
sleep "${BRONZE_DRAIN_SECONDS}"

log "Showing Bronze consumer lag before replay"
"${COMPOSE[@]}" exec -T redpanda \
  rpk group describe atmospulse-bronze-v1 -X brokers=redpanda:9092

log "Rebuilding canonical Silver from immutable Bronze"
"${COMPOSE[@]}" --profile pipeline run --rm spark \
  python -m streaming.jobs.normalize_events \
  --mode batch \
  --bronze-path '/opt/atmospulse/data/bronze/bluesky/event_date=*/event_hour=*/collection=*/*.jsonl.gz' \
  --output-path "/opt/atmospulse/data/silver-replay/${BENCH_SHA}"

log "Running deterministic failure tests, including the expected dbt failure"
uv run python scripts/run_failure_tests.py --include-dbt-failure

log "Injecting a broker outage"
uv run python -m reliability.compose_scenarios broker-outage \
  --outage-seconds "${BROKER_OUTAGE_SECONDS:-10}"

log "Injecting a Bronze consumer outage"
uv run python -m reliability.compose_scenarios consumer-outage \
  --outage-seconds "${CONSUMER_OUTAGE_SECONDS:-30}"

log "Restarting Spark"
uv run python -m reliability.compose_scenarios spark-restart

log "Replay and failure drills complete"
