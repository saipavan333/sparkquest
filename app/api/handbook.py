"""Handbook endpoints — serve the deep-dive markdown chapters to the in-app reader."""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException

from app.config import get_settings

router = APIRouter(prefix="/api/handbook", tags=["handbook"])

# Ordered table of contents: (slug, filename, display title).
CHAPTERS: list[tuple[str, str, str]] = [
    ("index", "README.md", "Overview & Syllabus"),
    ("architecture", "01-spark-architecture-and-execution.md", "1 · Spark Architecture & Execution"),
    ("performance", "02-performance-tuning.md", "2 · Performance Tuning & Debugging"),
    ("streaming", "03-streaming-internals.md", "3 · Structured Streaming Internals"),
    ("file-formats", "04-file-formats-and-layout.md", "4 · File Formats & Physical Layout"),
    ("lakehouse", "05-lakehouse-and-delta.md", "5 · Lakehouse & Delta Lake"),
    ("modeling", "06-data-modeling.md", "6 · Data Modeling for Analytics"),
    ("system-design", "07-system-design.md", "7 · Pipeline System Design"),
    ("python-sql", "08-python-and-sql-deep.md", "8 · Python & SQL Mastery"),
    ("joins-aqe", "09-joins-shuffle-aqe.md", "9 · Joins, Shuffle & AQE Deep-Dive"),
    ("rdd", "10-rdd-and-low-level-api.md", "10 · RDDs & the Low-Level API"),
    ("config", "11-configuration-and-cluster-sizing.md", "11 · Configuration & Cluster Sizing"),
    ("debugging", "12-debugging-and-spark-ui.md", "12 · Debugging & the Spark UI"),
    ("kafka", "13-kafka-and-streaming-io.md", "13 · Kafka & Streaming I/O"),
    ("iceberg", "14-apache-iceberg.md", "14 · Apache Iceberg"),
    ("questions", "interview-questions.md", "Interview Question Bank"),
    ("resources", "resources.md", "Resources & Study Plan"),
]
_BY_SLUG = {slug: fname for slug, fname, _ in CHAPTERS}
_BY_FILE = {fname: slug for slug, fname, _ in CHAPTERS}


def _handbook_dir() -> Path:
    return Path(get_settings().handbook_dir)


@router.get("")
def list_chapters():
    """Table of contents for the reader. `file` lets the client map in-text
    .md links to chapter slugs."""
    return {
        "chapters": [
            {"slug": slug, "title": title, "file": fname}
            for slug, fname, title in CHAPTERS
        ]
    }


@router.get("/{slug}")
def get_chapter(slug: str):
    fname = _BY_SLUG.get(slug)
    if not fname:
        raise HTTPException(status_code=404, detail="Chapter not found")
    path = _handbook_dir() / fname
    if not path.exists():
        raise HTTPException(status_code=404, detail="Chapter content unavailable")
    return {"slug": slug, "markdown": path.read_text(encoding="utf-8")}
