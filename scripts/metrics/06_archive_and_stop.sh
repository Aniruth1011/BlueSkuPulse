#!/usr/bin/env bash

# shellcheck disable=SC1091
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

BENCH_SHA="$(git rev-parse HEAD 2>/dev/null || printf 'uncommitted-workspace')"
ARCHIVE="${PROJECT_ROOT}/atmospulse-metrics-${BENCH_SHA}.tar.gz"
ITEMS=(benchmark/results)
[[ -d state/reconciliation ]] && ITEMS+=(state/reconciliation)

log "Archiving benchmark evidence"
tar -czf "${ARCHIVE}" "${ITEMS[@]}"

log "Stopping local services without deleting volumes"
"${COMPOSE[@]}" --profile pipeline stop

log "Evidence archive: ${ARCHIVE}"

