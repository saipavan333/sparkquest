"""Unit tests for the execution sandbox and auto-grader (Python only, fast)."""
from app.core.grader import grade
from app.core.models import Challenge


def _challenge(**overrides) -> Challenge:
    base = {"id": "t", "track": "python", "title": "t", "needs_spark": False,
            "solution_code": "", "checks": []}
    base.update(overrides)
    return Challenge(**base)


def test_stdout_contains_passes():
    c = _challenge(checks=[{"type": "stdout_contains", "value": "hi", "message": "m"}])
    result = grade(c, "print('hi there')")
    assert result.passed
    assert result.xp_awarded == 50


def test_var_equals_failure_does_not_pass():
    c = _challenge(checks=[{"type": "var_equals", "name": "x", "expected": 5, "message": "m"}])
    result = grade(c, "x = 4")
    assert not result.passed
    assert result.xp_awarded == 0


def test_runtime_error_is_reported_not_crash():
    c = _challenge(checks=[{"type": "var_equals", "name": "x", "expected": 5, "message": "m"}])
    result = grade(c, "x = 1 / 0")
    assert not result.passed
    assert "ZeroDivisionError" in (result.error or "")


def test_callable_returns_check():
    c = _challenge(checks=[{"type": "callable_returns", "name": "f", "args": [2, 3], "expected": 5, "message": "m"}])
    result = grade(c, "def f(a, b):\n    return a + b")
    assert result.passed


def test_empty_checks_never_passes():
    c = _challenge(checks=[])
    result = grade(c, "x = 1")
    assert not result.passed  # a submission with no checks cannot be graded as a pass


def test_infinite_loop_times_out_safely():
    c = _challenge(checks=[{"type": "var_equals", "name": "x", "expected": 1, "message": "m"}])
    # grade() uses the configured timeout; keep the loop trivially long.
    result = grade(c, "while True:\n    pass")
    assert not result.passed
    assert result.timed_out
