"""Structural tests for the lesson catalog (fast, no code execution)."""
from app.catalog import get_catalog


def test_catalog_loads_expected_count():
    assert len(get_catalog().challenges) == 16


def test_track_counts():
    by_track = get_catalog().by_track()
    assert len(by_track["python"]) == 6
    assert len(by_track["pyspark"]) == 7
    assert len(by_track["streaming"]) == 3


def test_ids_are_unique():
    ids = get_catalog().all_ids()
    assert len(ids) == len(set(ids))


def test_every_challenge_is_well_formed():
    for c in get_catalog().challenges:
        assert c.title, f"{c.id} missing title"
        assert c.brief.strip(), f"{c.id} missing brief"
        assert c.solution_code.strip(), f"{c.id} missing solution"
        assert c.checks, f"{c.id} has no grader checks"
        assert c.track in {"python", "pyspark", "streaming"}
        assert 1 <= c.difficulty <= 5


def test_public_challenge_hides_solution_and_checks():
    pub = get_catalog().public_challenge("py-01-hello")
    assert pub is not None
    assert "solution_code" not in pub
    assert "checks" not in pub
    assert "starter_code" in pub
