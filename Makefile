.PHONY: install lint typecheck test check up down topics ingest-fixture bronze-once spark-stream spark-replay smoke dbt-deps dbt-seed dbt-build dbt-docs phase5-check phase6-8-check phase9-10-check up-pipeline up-orchestration up-observability airflow-validate monitoring-validate terraform-fmt terraform-init terraform-validate docker-build benchmark-smoke reliability-tests

COMPOSE ?= docker-compose

install:
	uv sync --dev

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run mypy ingestion bronze streaming warehouse batch benchmarking reliability

test:
	uv run pytest --cov=ingestion --cov-report=term-missing

check: lint typecheck test

up:
	$(COMPOSE) up -d --wait redpanda console topic-init

up-pipeline:
	$(COMPOSE) --profile pipeline up -d --build redpanda topic-init ingestion bronze spark

down:
	$(COMPOSE) down

up-orchestration:
	test -n "$(AIRFLOW_DB_PASSWORD)"
	$(COMPOSE) --profile orchestration up -d --build redpanda topic-init postgres airflow-init airflow-api-server airflow-scheduler airflow-dag-processor airflow-statsd-exporter pushgateway

up-observability:
	test -n "$(GRAFANA_ADMIN_PASSWORD)"
	$(COMPOSE) --profile observability up -d prometheus alertmanager grafana airflow-statsd-exporter pushgateway

airflow-validate:
	$(COMPOSE) --profile orchestration run --rm airflow-scheduler airflow dags list-import-errors

monitoring-validate:
	$(COMPOSE) --profile observability run --rm --entrypoint promtool prometheus check config /etc/prometheus/prometheus.yml
	$(COMPOSE) --profile observability run --rm --entrypoint amtool alertmanager check-config /etc/alertmanager/alertmanager.yml

topics:
	$(COMPOSE) run --rm topic-init

ingest-fixture:
	ATMOSPULSE_SOURCE=fixture uv run python -m ingestion.producer

bronze-once:
	uv run python -m bronze.consumer --once

spark-stream:
	uv run python -m streaming.jobs.normalize_events --mode stream

spark-replay:
	test -n "$(BRONZE_PATH)" && test -n "$(OUTPUT_PATH)"
	uv run python -m streaming.jobs.normalize_events --mode batch --bronze-path "$(BRONZE_PATH)" --output-path "$(OUTPUT_PATH)"

dbt-deps:
	uv run dbt deps --project-dir dbt --profiles-dir dbt

dbt-seed:
	uv run dbt seed --full-refresh --project-dir dbt --profiles-dir dbt

dbt-build:
	uv run dbt build --exclude resource_type:seed --project-dir dbt --profiles-dir dbt

dbt-docs:
	uv run dbt docs generate --project-dir dbt --profiles-dir dbt

phase5-check: dbt-deps dbt-seed
	uv run dbt build --full-refresh --exclude resource_type:seed --project-dir dbt --profiles-dir dbt
	uv run dbt build --exclude resource_type:seed --project-dir dbt --profiles-dir dbt
	uv run dbt docs generate --project-dir dbt --profiles-dir dbt
	uv run pytest tests/unit/test_warehouse_loader.py

phase6-8-check: dbt-deps dbt-seed
	uv run dbt build --full-refresh --exclude resource_type:seed --project-dir dbt --profiles-dir dbt
	uv run dbt build --exclude resource_type:seed --project-dir dbt --profiles-dir dbt
	uv run pytest tests/unit/test_batch_control_plane.py tests/unit/test_phase6_8_assets.py
	uv run ruff check airflow batch monitoring tests/unit/test_batch_control_plane.py tests/unit/test_phase6_8_assets.py

terraform-fmt:
	terraform -chdir=infra/terraform fmt -recursive

terraform-init:
	terraform -chdir=infra/terraform init -backend=false

terraform-validate: terraform-init
	terraform -chdir=infra/terraform fmt -check -recursive
	terraform -chdir=infra/terraform validate

docker-build:
	docker build --file docker/app.Dockerfile --target ingestion --tag atmospulse-ingestion:local .
	docker build --file docker/app.Dockerfile --target bronze --tag atmospulse-bronze:local .
	docker build --file docker/app.Dockerfile --target spark --tag atmospulse-spark:local .
	docker build --file docker/app.Dockerfile --target warehouse --tag atmospulse-warehouse:local .
	docker build --file docker/airflow.Dockerfile --tag atmospulse-airflow:3.1.7 .

benchmark-smoke:
	uv run python -m benchmarking.run smoke --transport memory

reliability-tests:
	uv run python scripts/run_failure_tests.py --include-dbt-failure

phase9-10-check: lint typecheck terraform-validate reliability-tests benchmark-smoke

smoke:
	$(COMPOSE) exec redpanda rpk topic consume bluesky.raw.events -n 4 --brokers redpanda:9092
