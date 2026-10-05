#!/usr/bin/env bash

# shellcheck disable=SC1091
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

require_command uv
configure_java

log "Installing locked development dependencies"
uv sync --frozen --dev

log "Running formatting, linting, typing, Python, integration, and Spark tests"
uv run ruff format --check .
uv run ruff check .
uv run mypy ingestion bronze streaming warehouse batch benchmarking reliability
uv run pytest --cov=ingestion --cov-report=term-missing

log "Running deterministic dbt full-refresh and incremental validation"
uv run dbt deps --project-dir dbt --profiles-dir dbt
uv run dbt seed --full-refresh --project-dir dbt --profiles-dir dbt
uv run dbt build --full-refresh --exclude resource_type:seed --project-dir dbt --profiles-dir dbt
uv run dbt build --exclude resource_type:seed --project-dir dbt --profiles-dir dbt

log "Validation complete"

