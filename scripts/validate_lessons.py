"""Validate the curriculum: every challenge's reference solution must pass its
own grader checks. Run with an optional track filter:

    python scripts/validate_lessons.py            # all tracks
    python scripts/validate_lessons.py pyspark    # one track
"""
from __future__ import annotations

import sys
from pathlib import Path

# Make the repo root importable when run directly (python scripts/validate_lessons.py).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _included(challenge, filters: list[str]) -> bool:
    if not filters:
        return True
    return challenge.track in filters or any(f in challenge.id for f in filters)


def main(filters: list[str] | None = None) -> int:
    from app.catalog import get_catalog
    from app.core.grader import grade

    filters = filters or []
    catalog = get_catalog()
    failures = 0
    checked = 0
    for challenge in catalog.challenges:
        if not _included(challenge, filters):
            continue
        checked += 1
        result = grade(challenge, challenge.solution_code)
        status = "PASS" if result.passed else "FAIL"
        print(f"{status}  {challenge.id:26s} {result.duration_ms:6d}ms")
        if not result.passed:
            failures += 1
            if result.error:
                print(f"      error: {result.error}")
            for check in result.checks:
                if not check["passed"]:
                    print(f"      x {check['message']} | {check['detail']}")
    summary = "ALL PASS" if failures == 0 else f"{failures} FAILED"
    print("")
    print(f"Checked {checked} | {summary}")
    return failures


if __name__ == "__main__":
    sys.exit(1 if main(sys.argv[1:]) else 0)
