from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def git_commit() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


def environment_metadata() -> dict[str, Any]:
    return {
        "hostname": platform.node(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": sys.version.split()[0],
        "cpu_count": os.cpu_count(),
    }


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    run_id: str
    run_at: str
    benchmark_name: str
    status: str
    git_commit: str | None
    environment: dict[str, Any]
    parameters: dict[str, Any]
    measurements: dict[str, Any]
    method: str
    publishable: bool

    @classmethod
    def create(
        cls,
        *,
        benchmark_name: str,
        parameters: dict[str, Any],
        measurements: dict[str, Any],
        method: str,
        target_run: bool,
    ) -> BenchmarkResult:
        now = datetime.now(UTC)
        commit = git_commit()
        publishable = target_run and commit is not None
        return cls(
            run_id=f"{benchmark_name}-{now.strftime('%Y%m%dT%H%M%S%fZ')}",
            run_at=now.isoformat(),
            benchmark_name=benchmark_name,
            status="MEASURED" if publishable else "VALIDATION",
            git_commit=commit,
            environment=environment_metadata(),
            parameters=parameters,
            measurements=measurements,
            method=method,
            publishable=publishable,
        )

    def write(self, output_dir: Path) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        target = output_dir / f"{self.run_id}.json"
        fd, temporary = tempfile.mkstemp(prefix=".benchmark-", dir=output_dir, text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(asdict(self), handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return target
