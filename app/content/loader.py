"""Load the lesson catalog from YAML files under ``lessons/``."""
from __future__ import annotations

from pathlib import Path

import yaml

from app.core.models import Challenge, TrackMeta

TRACKS: list[TrackMeta] = [
    TrackMeta(id="python", title="Python for Data Engineering", subtitle="Zero to comfortable", order=1),
    TrackMeta(id="pyspark", title="PySpark Foundations & DataFrames", subtitle="Think in DataFrames", order=2),
    TrackMeta(id="performance", title="Performance & Internals", subtitle="Make Spark fast", order=3),
    TrackMeta(id="streaming", title="Structured Streaming", subtitle="Real-time pipelines", order=4),
    TrackMeta(id="delta", title="Lakehouse & Delta Lake", subtitle="ACID on the lake", order=5),
    TrackMeta(id="capstone", title="Capstone ETL Projects", subtitle="Put it all together", order=6),
]
_TRACK_ORDER = {t.id: t.order for t in TRACKS}


class Catalog:
    """In-memory index of all challenges, sorted into a learning path."""

    def __init__(self, challenges: list[Challenge]) -> None:
        self.challenges = challenges
        self._by_id = {c.id: c for c in challenges}

    @classmethod
    def load(cls, root: str | Path) -> Catalog:
        root = Path(root)
        items: list[Challenge] = []
        seen: set[str] = set()
        for path in sorted(root.rglob("*.yaml")):
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
            if not data:
                continue
            challenge = Challenge(**data)
            if challenge.id in seen:
                raise ValueError(f"Duplicate challenge id: {challenge.id} ({path})")
            seen.add(challenge.id)
            items.append(challenge)
        items.sort(key=lambda c: (_TRACK_ORDER.get(c.track, 99), c.order, c.id))
        return cls(items)

    def get(self, challenge_id: str) -> Challenge | None:
        return self._by_id.get(challenge_id)

    def all_ids(self) -> list[str]:
        return [c.id for c in self.challenges]

    def by_track(self) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {}
        for c in self.challenges:
            out.setdefault(c.track, []).append(c.id)
        return out

    def public_tracks(self) -> list[dict]:
        """Catalog for the frontend. Excludes solutions and grader internals."""
        by_track = {t.id: [] for t in TRACKS}
        for c in self.challenges:
            by_track.setdefault(c.track, []).append(
                {
                    "id": c.id,
                    "title": c.title,
                    "order": c.order,
                    "difficulty": c.difficulty,
                    "xp": c.xp,
                    "concepts": c.concepts,
                    "needs_spark": c.needs_spark,
                }
            )
        return [
            {
                "id": t.id,
                "title": t.title,
                "subtitle": t.subtitle,
                "order": t.order,
                "challenges": by_track.get(t.id, []),
            }
            for t in sorted(TRACKS, key=lambda t: t.order)
        ]

    def public_challenge(self, challenge_id: str) -> dict | None:
        """Single challenge for the frontend (brief, starter code, hints — no solution/checks)."""
        c = self.get(challenge_id)
        if not c:
            return None
        return {
            "id": c.id,
            "track": c.track,
            "title": c.title,
            "order": c.order,
            "difficulty": c.difficulty,
            "xp": c.xp,
            "concepts": c.concepts,
            "brief": c.brief,
            "needs_spark": c.needs_spark,
            "starter_code": c.starter_code,
            "hints": c.hints,
        }
