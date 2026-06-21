"""Load the interview question bank from YAML files under ``questions/``."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml

from app.config import get_settings

# Ordered category slugs -> display titles.
CATEGORY_TITLES: dict[str, str] = {
    "spark-core": "Spark Core",
    "performance": "Performance & Tuning",
    "streaming": "Structured Streaming",
    "lakehouse": "Lakehouse & Delta",
    "modeling": "Data Modeling",
    "sql": "SQL",
    "python": "Python",
    "system-design": "System Design",
    "behavioral": "Behavioral",
}


@lru_cache
def load_questions() -> list[dict]:
    root = Path(get_settings().questions_dir)
    out: list[dict] = []
    for path in sorted(root.glob("*.yaml")):
        category = path.stem
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or []
        for item in data:
            out.append(
                {
                    "id": item["id"],
                    "category": category,
                    "category_title": CATEGORY_TITLES.get(category, category.title()),
                    "difficulty": item.get("difficulty", 3),
                    "q": item["q"],
                    "a": item["a"],
                    "tags": item.get("tags", []),
                }
            )
    return out
