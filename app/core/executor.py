"""Process-isolated code executor.

Each submission runs in a fresh child process with a hard wall-clock timeout and
truncated output. The parent communicates with the child only through two JSON
files, so a hang, crash, or OOM in learner code cannot take down the API.

Security model (MVP): process isolation + timeout + output caps. This is *not*
a hostile-multi-tenant sandbox. The documented production path is to run each
submission in an ephemeral, network-isolated container (gVisor / nsjail) — see
docs/ARCHITECTURE.md.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
HARNESS = Path(__file__).resolve().parent / "harness.py"


@dataclass
class ExecResult:
    ran: bool
    error: str | None
    traceback: str | None
    stdout: str
    stderr: str
    checks: list = field(default_factory=list)
    duration_ms: int = 0
    spark_startup_ms: int = 0
    timed_out: bool = False


def run_job(
    user_code: str,
    *,
    needs_spark: bool = False,
    needs_delta: bool = False,
    checks: list | None = None,
    spark_master: str = "local[2]",
    timeout: int = 25,
    max_output_chars: int = 20000,
) -> ExecResult:
    """Execute ``user_code`` in a child process and return a structured result."""
    checks = checks or []
    with tempfile.TemporaryDirectory(prefix="sq_exec_") as tmp:
        job_path = os.path.join(tmp, "job.json")
        res_path = os.path.join(tmp, "result.json")
        job = {
            "user_code": user_code,
            "needs_spark": needs_spark,
            "needs_delta": needs_delta,
            "spark_master": spark_master,
            "checks": checks,
            "max_output_chars": max_output_chars,
            "repo_root": str(REPO_ROOT),
        }
        with open(job_path, "w", encoding="utf-8") as fh:
            json.dump(job, fh)

        env = dict(os.environ)
        env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        env.setdefault("PYSPARK_PYTHON", sys.executable)
        env.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        # Pin Spark to loopback so it never depends on resolving the machine
        # hostname (which fails in many containers/sandboxes). Correct for local mode.
        env.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

        t0 = time.time()
        try:
            subprocess.run(
                [sys.executable, str(HARNESS), job_path, res_path],
                timeout=timeout,
                capture_output=True,
                env=env,
                cwd=str(REPO_ROOT),
                check=False,
            )
        except subprocess.TimeoutExpired:
            return ExecResult(
                ran=False,
                error=f"Execution exceeded the {timeout}s time limit. "
                "Check for an infinite loop or an action over too much data.",
                traceback=None,
                stdout="",
                stderr="",
                duration_ms=int((time.time() - t0) * 1000),
                timed_out=True,
            )

        if not os.path.exists(res_path):
            return ExecResult(
                ran=False,
                error="The process exited without producing a result "
                "(it may have crashed or run out of memory).",
                traceback=None,
                stdout="",
                stderr="",
                duration_ms=int((time.time() - t0) * 1000),
            )

        with open(res_path, encoding="utf-8") as fh:
            data = json.load(fh)

        return ExecResult(
            ran=data["ran"],
            error=data["error"],
            traceback=data["traceback"],
            stdout=data["stdout"],
            stderr=data["stderr"],
            checks=data["checks"],
            duration_ms=data["duration_ms"],
            spark_startup_ms=data.get("spark_startup_ms", 0),
        )
