"""Structural tests for the lesson catalog (fast, no code execution)."""
from pathlib import Path

from app.catalog import get_catalog
from app.config import get_settings
from app.content.loader import TRACKS

_VALID_TRACKS = {t.id for t in TRACKS}


def test_catalog_matches_lesson_files():
    n_files = len(list(Path(get_settings().lessons_dir).rglob("*.yaml")))
    challenges = get_catalog().challenges
    assert len(challenges) == n_files
    assert len(challenges) >= 50  # curriculum has grown well past the MVP


def test_live_tracks_have_lessons():
    by_track = get_catalog().by_track()
    assert set(by_track) <= _VALID_TRACKS
    for track in ("python", "pyspark", "performance", "streaming", "capstone"):
        assert by_track.get(track), f"track {track} has no lessons"


def test_ids_are_unique():
    ids = get_catalog().all_ids()
    assert len(ids) == len(set(ids))


def test_every_challenge_is_well_formed():
    for c in get_catalog().challenges:
        assert c.title, f"{c.id} missing title"
        assert c.brief.strip(), f"{c.id} missing brief"
        assert c.solution_code.strip(), f"{c.id} missing solution"
        assert c.checks, f"{c.id} has no grader checks"
        assert c.track in _VALID_TRACKS, f"{c.id} has unknown track {c.track}"
        assert 1 <= c.difficulty <= 5


def test_public_challenge_hides_solution_and_checks():
    pub = get_catalog().public_challenge("py-01-hello")
    assert pub is not None
    assert "solution_code" not in pub
    assert "checks" not in pub
    assert "starter_code" in pub
