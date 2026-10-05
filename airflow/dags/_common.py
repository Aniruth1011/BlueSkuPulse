from __future__ import annotations

from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator

PROJECT_ROOT = "/opt/airflow/atmospulse"
DBT_FLAGS = "--project-dir dbt --profiles-dir dbt --target ${ATMOSPULSE_DBT_TARGET:-bigquery}"


def bounded_bash(task_id: str, command: str, *, retries: int = 2) -> BashOperator:
    wrapped = (
        f'python -m batch.task_runner --task "{task_id}" '
        '--run-id "$ATMOSPULSE_AIRFLOW_RUN_ID" -- '
        f"{command}"
    )
    return BashOperator(
        task_id=task_id,
        bash_command=wrapped,
        cwd=PROJECT_ROOT,
        env={
            "ATMOSPULSE_AIRFLOW_RUN_ID": "{{ run_id }}",
            "ATMOSPULSE_DATA_INTERVAL_START": "{{ data_interval_start.isoformat() }}",
            "ATMOSPULSE_DATA_INTERVAL_END": "{{ data_interval_end.isoformat() }}",
        },
        append_env=True,
        retries=retries,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=2),
    )
