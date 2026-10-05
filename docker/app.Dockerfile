# syntax=docker/dockerfile:1.7
FROM python:3.12-slim-bookworm AS runtime

ARG APP_UID=10001
ARG APP_GID=10001

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/atmospulse/.venv \
    PATH=/opt/atmospulse/.venv/bin:${PATH}

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates openjdk-17-jre-headless tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid "${APP_GID}" atmospulse \
    && useradd --uid "${APP_UID}" --gid "${APP_GID}" --create-home atmospulse

COPY --from=ghcr.io/astral-sh/uv:0.8.22 /uv /uvx /bin/
WORKDIR /opt/atmospulse
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY ingestion ingestion
COPY bronze bronze
COPY streaming streaming
COPY warehouse warehouse
COPY batch batch
COPY benchmarking benchmarking
COPY dbt dbt
RUN mkdir -p /opt/atmospulse/state /opt/atmospulse/data \
    && chown -R atmospulse:atmospulse /opt/atmospulse

USER atmospulse
ENTRYPOINT ["/usr/bin/tini", "--"]

FROM runtime AS ingestion
EXPOSE 9108
CMD ["python", "-m", "ingestion.producer"]

FROM runtime AS bronze
EXPOSE 9109
CMD ["python", "-m", "bronze.consumer"]

FROM runtime AS spark
EXPOSE 9110
CMD ["python", "-m", "streaming.jobs.normalize_events", "--mode", "stream"]

FROM runtime AS warehouse
CMD ["python", "-m", "warehouse.loader", "--help"]
