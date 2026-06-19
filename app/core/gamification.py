"""Gamification: XP, levels, badges, and an in-memory progress store.

The store is intentionally simple (thread-safe in-memory dict) so the demo runs
with zero infrastructure. The interface mirrors what a persistent backend would
expose, so swapping in Redis or Postgres is a drop-in change (see
docs/ARCHITECTURE.md, "Persistence").
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field

# Cumulative XP required to *reach* each level (index 0 -> level 1).
_LEVEL_THRESHOLDS = [0, 150, 400, 750, 1200, 1800, 2500, 3300, 4200, 5200]

# id -> (display name, description, emoji)
BADGES: dict[str, tuple[str, str, str]] = {
    "first_blood": ("First Blood", "Solved your first challenge", "🩸"),
    "pythonista": ("Pythonista", "Completed the Python Foundations track", "🐍"),
    "spark_wrangler": ("Spark Wrangler", "Completed the PySpark & Spark SQL track", "⚡"),
    "stream_master": ("Stream Master", "Completed the Structured Streaming track", "🌊"),
    "rising_star": ("Rising Star", "Reached level 5", "⭐"),
    "grand_master": ("Grand Master", "Solved every challenge in SparkQuest", "👑"),
}


def level_for_xp(xp: int) -> int:
    level = 1
    for i, threshold in enumerate(_LEVEL_THRESHOLDS):
        if xp >= threshold:
            level = i + 1
    return level


def xp_to_next_level(xp: int) -> int | None:
    for threshold in _LEVEL_THRESHOLDS:
        if xp < threshold:
            return threshold - xp
    return None  # max level reached


@dataclass
class UserProgress:
    user_id: str
    xp: int = 0
    solved: set[str] = field(default_factory=set)
    badges: set[str] = field(default_factory=set)

    @property
    def level(self) -> int:
        return level_for_xp(self.xp)


def evaluate_badges(progress: UserProgress, by_track: dict[str, list[str]], all_ids: list[str]) -> set[str]:
    """Return the full set of badge ids the user currently qualifies for."""
    earned: set[str] = set()
    if len(progress.solved) >= 1:
        earned.add("first_blood")
    track_badge = {"python": "pythonista", "pyspark": "spark_wrangler", "streaming": "stream_master"}
    for track, badge in track_badge.items():
        ids = by_track.get(track) or []
        if ids and set(ids) <= progress.solved:
            earned.add(badge)
    if progress.level >= 5:
        earned.add("rising_star")
    if all_ids and set(all_ids) <= progress.solved:
        earned.add("grand_master")
    return earned


class ProgressStore:
    """Thread-safe in-memory progress store."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._users: dict[str, UserProgress] = {}

    def get(self, user_id: str) -> UserProgress:
        with self._lock:
            return self._users.setdefault(user_id, UserProgress(user_id))

    def record_solve(
        self,
        user_id: str,
        challenge_id: str,
        xp: int,
        by_track: dict[str, list[str]],
        all_ids: list[str],
    ) -> tuple[int, int, list[str]]:
        """Record a solved challenge. Returns (total_xp, level, newly_earned_badges)."""
        with self._lock:
            user = self._users.setdefault(user_id, UserProgress(user_id))
            first_time = challenge_id not in user.solved
            if first_time:
                user.solved.add(challenge_id)
                user.xp += xp
            before = set(user.badges)
            user.badges = evaluate_badges(user, by_track, all_ids)
            new_badges = sorted(user.badges - before)
            return user.xp, user.level, new_badges

    def leaderboard(self, limit: int = 20) -> list[dict]:
        with self._lock:
            ranked = sorted(self._users.values(), key=lambda u: u.xp, reverse=True)
            return [
                {
                    "user_id": u.user_id,
                    "xp": u.xp,
                    "level": u.level,
                    "solved": len(u.solved),
                    "badges": len(u.badges),
                }
                for u in ranked[:limit]
                if u.xp > 0
            ]


# Process-wide singleton.
store = ProgressStore()
