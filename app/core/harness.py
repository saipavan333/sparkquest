"""Standalone execution harness — runs in a child process, never imported by the API.

Usage:
    python harness.py <job.json> <result.json>

It executes learner-submitted code in an isolated namespace (optionally with a
Spark session injected as ``spark``), captures stdout/stderr, evaluates a list
of declarative checks, and writes a JSON verdict to ``result.json``. The parent
process enforces the wall-clock timeout and reads only the result file, so a
crash or hang in learner code cannot corrupt the server.
"""
from __future__ import annotations

import collections
import contextlib
import io
import json
import sys
import time
import traceback
import types
from typing import Any

_MISSING = object()


def _multiset_equal(got: list, expected: list) -> bool:
    """Order-insensitive equality for lists of row dicts."""

    def key(d):
        return json.dumps(d, sort_keys=True, default=str)

    return collections.Counter(key(r) for r in got) == collections.Counter(
        key(r) for r in expected
    )


def evaluate_check(check: dict, ns: dict, stdout_text: str) -> dict:
    """Evaluate one declarative check against the post-execution namespace."""
    ctype = check.get("type")
    msg = check.get("message", ctype or "check")
    try:
        if ctype == "stdout_contains":
            ok = str(check["value"]) in stdout_text
        elif ctype == "stdout_equals":
            ok = stdout_text.strip() == str(check["value"]).strip()
        elif ctype == "var_equals":
            ok = ns.get(check["name"], _MISSING) == check["expected"]
        elif ctype == "var_close":
            val = ns.get(check["name"])
            ok = val is not None and abs(float(val) - float(check["expected"])) <= float(
                check.get("tol", 1e-6)
            )
        elif ctype == "var_type":
            ok = type(ns.get(check["name"])).__name__ == check["type_name"]
        elif ctype == "callable_returns":
            fn = ns.get(check["name"])
            ok = callable(fn) and fn(*check.get("args", [])) == check["expected"]
        elif ctype == "df_columns":
            df = ns.get(check["name"])
            ok = df is not None and list(df.columns) == list(check["expected"])
        elif ctype == "df_row_count":
            df = ns.get(check["name"])
            ok = df is not None and df.count() == int(check["expected"])
        elif ctype == "df_equals":
            df = ns.get(check["name"])
            if df is None:
                ok = False
            else:
                got = [r.asDict(recursive=True) for r in df.collect()]
                exp = list(check["expected"])
                ok = got == exp if check.get("ordered") else _multiset_equal(got, exp)
        elif ctype == "custom":
            scope: dict[str, Any] = {"ns": ns, "stdout": stdout_text, "ok": False}
            exec(check["code"], {}, scope)  # noqa: S102 (trusted, author-written)
            ok = bool(scope.get("ok"))
        else:
            return {"passed": False, "message": msg, "detail": f"unknown check type: {ctype}"}
        return {"passed": bool(ok), "message": msg, "detail": "" if ok else check.get("fail_detail", "")}
    except Exception as exc:  # noqa: BLE001 — surface any check failure to the learner
        return {"passed": False, "message": msg, "detail": f"{type(exc).__name__}: {exc}"}


def main() -> None:
    job_path, result_path = sys.argv[1], sys.argv[2]
    with open(job_path, encoding="utf-8") as fh:
        job = json.load(fh)

    user_code = job["user_code"]
    needs_spark = job.get("needs_spark", False)
    master = job.get("spark_master", "local[2]")
    checks = job.get("checks", [])
    max_output = int(job.get("max_output_chars", 20000))

    result = {
        "ran": False,
        "error": None,
        "traceback": None,
        "stdout": "",
        "stderr": "",
        "checks": [],
        "duration_ms": 0,
        "spark_startup_ms": 0,
    }
    # Back the namespace with a real module registered in sys.modules so that
    # features relying on module introspection (e.g. @dataclass) work correctly.
    _module = types.ModuleType("__sparkquest__")
    sys.modules["__sparkquest__"] = _module
    ns: dict[str, Any] = _module.__dict__
    ns["__name__"] = "__sparkquest__"
    t0 = time.time()
    spark = None

    if needs_spark:
        try:
            sys.path.insert(0, job["repo_root"])
            from app.core.spark_session import build_spark_session

            ts = time.time()
            spark = build_spark_session(master=master, app_name="sparkquest-grader")
            result["spark_startup_ms"] = int((time.time() - ts) * 1000)
            from pyspark.sql import functions as F  # noqa: N812
            from pyspark.sql import types as T  # noqa: N812

            ns.update({"spark": spark, "F": F, "T": T})
        except Exception as exc:  # noqa: BLE001
            result["error"] = f"Failed to start Spark: {exc}"
            result["traceback"] = traceback.format_exc()
            with open(result_path, "w", encoding="utf-8") as fh:
                json.dump(result, fh)
            return

    out_buf, err_buf = io.StringIO(), io.StringIO()
    try:
        with contextlib.redirect_stdout(out_buf), contextlib.redirect_stderr(err_buf):
            exec(compile(user_code, "<submission>", "exec"), ns, ns)  # noqa: S102
        result["ran"] = True
    except Exception as exc:  # noqa: BLE001 — learner errors are expected
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["traceback"] = traceback.format_exc()
    finally:
        result["stdout"] = out_buf.getvalue()[:max_output]
        result["stderr"] = err_buf.getvalue()[:max_output]

    if result["ran"]:
        for chk in checks:
            result["checks"].append(evaluate_check(chk, ns, result["stdout"]))

    if spark is not None:
        with contextlib.suppress(Exception):
            spark.stop()

    result["duration_ms"] = int((time.time() - t0) * 1000)
    with open(result_path, "w", encoding="utf-8") as fh:
        json.dump(result, fh)


if __name__ == "__main__":
    main()
