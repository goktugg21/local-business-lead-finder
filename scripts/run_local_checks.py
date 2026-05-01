"""Run local quality checks: compileall, pytest, and verify_workbook (if a
workbook exists). Exits non-zero if any step fails so CI/devs see a clear
fail signal.

Usage:
    python scripts/run_local_checks.py
    python scripts/run_local_checks.py --run-log amsterdam_run.log --expected-city "Amsterdam"
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def _run(cmd: list[str], label: str) -> int:
    print(f"\n=== {label} ===")
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd)
    status = "OK  " if result.returncode == 0 else "FAIL"
    print(f"[{status}] {label} (exit={result.returncode})")
    return result.returncode


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run compileall + pytest + verify_workbook for the current workspace."
    )
    parser.add_argument(
        "--run-log",
        type=Path,
        default=None,
        help="Pass through to verify_workbook.py as --run-log.",
    )
    parser.add_argument(
        "--expected-city",
        default=None,
        help="Pass through to verify_workbook.py as --expected-city. Only meaningful with --run-log.",
    )
    args = parser.parse_args()

    if args.expected_city and not args.run_log:
        print(
            "ERROR: --expected-city is only meaningful with --run-log. "
            "Provide both, or neither.",
            file=sys.stderr,
        )
        return 2

    repo_root = Path(__file__).resolve().parent.parent
    failures = 0

    failures += _run([sys.executable, "-m", "compileall", str(repo_root)], "compileall")
    failures += _run([sys.executable, "-m", "pytest", "-q"], "pytest")

    workbook = repo_root / "output" / "latest.xlsx"
    if workbook.exists():
        verify_cmd: list[str] = [
            sys.executable,
            str(repo_root / "scripts" / "verify_workbook.py"),
        ]
        if args.run_log is not None:
            verify_cmd.extend(["--run-log", str(args.run_log)])
        if args.expected_city is not None:
            verify_cmd.extend(["--expected-city", args.expected_city])
        failures += _run(verify_cmd, "verify_workbook")
    else:
        print("\n=== verify_workbook ===")
        print("[SKIP] output/latest.xlsx not found; skipping workbook verification.")

    print()
    if failures == 0:
        print("All local checks passed.")
        return 0
    print(f"{failures} local check(s) failed.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
