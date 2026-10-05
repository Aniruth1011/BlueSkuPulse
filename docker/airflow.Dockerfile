FROM apache/airflow:3.1.7-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java-home \
    && python -m venv /opt/atmospulse-venv \
    && chown -R airflow:root /opt/atmospulse-venv

USER airflow
COPY docker/airflow-requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir statsd==4.0.1 \
    && /opt/atmospulse-venv/bin/pip install --no-cache-dir -r /tmp/airflow-requirements.txt

ENV PYTHONPATH=/opt/airflow/atmospulse \
    JAVA_HOME=/opt/java-home \
    PATH=/opt/atmospulse-venv/bin:/home/airflow/.local/bin:${PATH}
