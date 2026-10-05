from __future__ import annotations

import subprocess
from pathlib import Path

from benchmarking.run import SCENARIOS


def test_benchmark_profiles_encode_required_workloads() -> None:
    assert SCENARIOS["sustained"].count == 1_800_000
    assert SCENARIOS["sustained"].rate == 1_000
    assert SCENARIOS["burst"].count == 1_500_000
    assert SCENARIOS["burst"].rate == 5_000
    assert SCENARIOS["replay"].count >= 5_000_000
    assert SCENARIOS["duplicates"].duplicate_fraction == 0.10
    assert not SCENARIOS["smoke"].target_run


def test_terraform_separates_resource_and_relation_ownership() -> None:
    root = Path("infra/terraform")
    expected = {
        "storage.tf",
        "bigquery.tf",
        "iam.tf",
        "secrets.tf",
        "monitoring.tf",
        "variables.tf",
        "outputs.tf",
        ".terraform.lock.hcl",
    }
    assert expected <= {path.name for path in root.iterdir()}
    storage = (root / "storage.tf").read_text()
    bigquery = (root / "bigquery.tf").read_text()
    secrets = (root / "secrets.tf").read_text()
    assert 'public_access_prevention    = "enforced"' in storage
    assert 'var.environment == "prod" ? false' in storage
    assert "silver_events" not in bigquery  # owned by the schema-evolving loader
    assert "google_secret_manager_secret_version" not in secrets


def test_ci_covers_code_data_infrastructure_and_images() -> None:
    ci = Path(".github/workflows/ci.yml").read_text()
    release = Path(".github/workflows/release.yml").read_text()
    for required in ("ruff check", "mypy", "pytest", "dbt build", "terraform", "docker/build"):
        assert required in ci
    assert "provenance: true" in release
    assert "sbom: true" in release


def test_metrics_shell_entrypoints_are_syntax_valid_and_cost_guarded() -> None:
    scripts = sorted(Path("scripts/metrics").glob("*.sh"))
    assert scripts
    for script in scripts:
        subprocess.run(["bash", "-n", str(script)], check=True)
    assert "CONFIRM_TERRAFORM_APPLY" in Path("scripts/metrics/04_gcp_infrastructure.sh").read_text()
    assert "CONFIRM_GCS_UPLOAD" in Path("scripts/metrics/05_cloud_load.sh").read_text()
    assert "CONFIRM_BIGQUERY_COST" in Path("scripts/metrics/05_bigquery_benchmark.sh").read_text()
