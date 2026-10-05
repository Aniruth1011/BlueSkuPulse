#!/usr/bin/env bash

set -euo pipefail

METRICS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${METRICS_DIR}/../.." && pwd)"
cd "${PROJECT_ROOT}"

if [[ -f "${METRICS_DIR}/metrics.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${METRICS_DIR}/metrics.env"
  set +a
fi

if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  COMPOSE=(docker compose)
fi

log() {
  printf '\n[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Required command is missing: $1"
}

configure_java() {
  if [[ -z "${JAVA_HOME:-}" ]] && [[ -d /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ]]; then
    export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
    export PATH="/opt/homebrew/opt/openjdk@17/bin:${PATH}"
  fi
}

require_clean_commit() {
  git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die \
    "Initialize Git and commit the project before a publishable benchmark."
  [[ -z "$(git status --porcelain)" ]] || die \
    "The Git worktree is not clean. Commit the benchmark code before running full metrics."
  export BENCH_SHA
  BENCH_SHA="$(git rev-parse HEAD)"
}

terraform_output() {
  terraform -chdir=infra/terraform output -json "$1"
}

