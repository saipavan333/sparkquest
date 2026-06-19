"""Process-wide singleton accessor for the loaded lesson catalog."""
from __future__ import annotations

from functools import lru_cache

from app.config import get_settings
from app.content.loader import Catalog


@lru_cache
def get_catalog() -> Catalog:
    return Catalog.load(get_settings().lessons_dir)
