"""Every reference solution must pass its own grader checks.

Python lessons run by default; Spark/streaming lessons are marked ``spark`` so a
fast CI lane can deselect them with ``-m 'not spark'`` while a full lane runs all.
"""
import pytest

from app.catalog import get_catalog
from app.core.grader import grade

_catalog = get_catalog()
_python_ids = [c.id for c in _catalog.challenges if not c.needs_spark]
_spark_ids = [c.id for c in _catalog.challenges if c.needs_spark and not c.needs_delta]
_delta_ids = [c.id for c in _catalog.challenges if c.needs_delta]


@pytest.mark.parametrize("challenge_id", _python_ids)
def test_python_reference_solution_passes(challenge_id):
    challenge = _catalog.get(challenge_id)
    result = grade(challenge, challenge.solution_code)
    assert result.passed, f"{challenge_id} failed: {result.error or result.checks}"


@pytest.mark.spark
@pytest.mark.parametrize("challenge_id", _spark_ids)
def test_spark_reference_solution_passes(challenge_id):
    challenge = _catalog.get(challenge_id)
    result = grade(challenge, challenge.solution_code)
    assert result.passed, f"{challenge_id} failed: {result.error or result.checks}"


@pytest.mark.delta
@pytest.mark.parametrize("challenge_id", _delta_ids)
def test_delta_reference_solution_passes(challenge_id):
    # Needs Delta's Maven JAR — runs in the dedicated CI 'delta' job, not the
    # offline sandbox. Deselected from the main lane via -m 'not delta'.
    challenge = _catalog.get(challenge_id)
    result = grade(challenge, challenge.solution_code)
    assert result.passed, f"{challenge_id} failed: {result.error or result.checks}"
