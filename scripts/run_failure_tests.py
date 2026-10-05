from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time


def main() -> int:
    parser = argparse.ArgumentParser(description="Run deterministic AtmosPulse failure tests")
    parser.add_argument("--include-dbt-failure", action="store_true")
    args = parser.parse_args()
    started = time.monotonic()
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/reliability", "-q"], check=False
    )
    if result.returncode != 0:
        return result.returncode
    if args.include_dbt_failure:
        dbt_executable = shutil.which("dbt")
        if dbt_executable is None:
            print("ERROR: dbt executable is not available", file=sys.stderr)
            return 1
        dbt = subprocess.run(
            [
                dbt_executable,
                "test",
                "--select",
                "forced_failure_injection",
                "--vars",
                "{inject_failure: true}",
                "--project-dir",
                "dbt",
                "--profiles-dir",
                "dbt",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        output = f"{dbt.stdout}\n{dbt.stderr}"
        if dbt.returncode == 0 or "forced_failure_injection" not in output:
            print(output)
            print(
                "ERROR: named dbt failure was not detected (an infrastructure error does not pass)",
                file=sys.stderr,
            )
            return 1
        print(dbt.stdout)
        print("Controlled dbt failure was detected as expected.")
    print(f"Failure-test wall time: {time.monotonic() - started:.3f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
