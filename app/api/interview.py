"""Mock-interview endpoints — serve the question bank to the in-app drill mode."""
from __future__ import annotations

from collections import Counter

from fastapi import APIRouter

from app.content.questions import CATEGORY_TITLES, load_questions

router = APIRouter(prefix="/api/interview", tags=["interview"])


@router.get("")
def all_questions():
    """The whole bank plus category metadata. The client filters by category,
    shuffles, reveals answers, and tracks a self-scored session."""
    questions = load_questions()
    counts = Counter(q["category"] for q in questions)
    order = list(CATEGORY_TITLES.keys())
    categories = sorted(
        ({"slug": c, "title": CATEGORY_TITLES.get(c, c), "count": n} for c, n in counts.items()),
        key=lambda x: order.index(x["slug"]) if x["slug"] in order else 99,
    )
    return {"total": len(questions), "categories": categories, "questions": questions}
