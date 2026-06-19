"""Grading: run a submission, decide pass/fail, and award XP."""
from __future__ import annotations

from dataclasses import dataclass, field

from app.config import get_settings
from app.core.executor import ExecResult, run_job
from app.core.models import Challenge


@dataclass
class GradeResult:
    passed: bool
    checks: list = field(default_factory=list)
    stdout: str = ""
    stderr: str = ""
    error: str | None = None
    traceback: str | None = None
    duration_ms: int = 0
    spark_startup_ms: int = 0
    xp_awarded: int = 0
    timed_out: bool = False


def _timeout_for(challenge: Challenge) -> int:
    s = get_settings()
    return s.spark_exec_timeout if challenge.needs_spark else s.exec_timeout


def grade(challenge: Challenge, code: str) -> GradeResult:
    """Execute and grade a submission for ``challenge``."""
    s = get_settings()
    res = run_job(
        code,
        needs_spark=challenge.needs_spark,
        checks=challenge.checks,
        spark_master=s.spark_master,
        timeout=_timeout_for(challenge),
        max_output_chars=s.max_output_chars,
    )
    # A submission passes only if it ran cleanly and every check passed.
    passed = res.ran and len(res.checks) > 0 and all(c["passed"] for c in res.checks)
    return GradeResult(
        passed=passed,
        checks=res.checks,
        stdout=res.stdout,
        stderr=res.stderr,
        error=res.error,
        traceback=res.traceback,
        duration_ms=res.duration_ms,
        spark_startup_ms=res.spark_startup_ms,
        xp_awarded=challenge.xp if passed else 0,
        timed_out=res.timed_out,
    )


def run_only(challenge: Challenge, code: str) -> ExecResult:
    """Execute a submission without grading (the 'Run' button)."""
    s = get_settings()
    return run_job(
        code,
        needs_spark=challenge.needs_spark,
        checks=[],
        spark_master=s.spark_master,
        timeout=_timeout_for(challenge),
        max_output_chars=s.max_output_chars,
    )
