"""Load the interview question bank from YAML files under ``questions/``."""
from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

import yaml

from app.config import get_settings

# Ordered category slugs -> display titles. Files may be split (e.g. spark-core-2.yaml);
# a trailing -<number> is stripped so they fold into the same category.
CATEGORY_TITLES: dict[str, str] = {
    "spark-core": "Spark Core",
    "performance": "Performance & Tuning",
    "streaming": "Structured Streaming",
    "kafka": "Kafka & Streaming Infra",
    "file-formats": "File Formats & Layout",
    "lakehouse": "Lakehouse & Delta",
    "modeling": "Data Modeling",
    "sql": "SQL",
    "python": "Python",
    "system-design": "System Design",
    "orchestration": "Orchestration & Ops",
    "coding": "Coding Patterns",
    "scenarios": "Scenarios & Troubleshooting",
    "behavioral": "Behavioral",
}


@lru_cache
def load_questions() -> list[dict]:
    root = Path(get_settings().questions_dir)
    out: list[dict] = []
    for path in sorted(root.glob("*.yaml")):
        category = re.sub(r"-\d+$", "", path.stem)
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
