"""Progress, leaderboard, and badge catalog endpoints."""
from __future__ import annotations

from fastapi import APIRouter

from app.core.gamification import BADGES, store, xp_to_next_level

router = APIRouter(prefix="/api", tags=["progress"])


@router.get("/progress/{user_id}")
def progress(user_id: str):
    user = store.get(user_id)
    return {
        "user_id": user.user_id,
        "xp": user.xp,
        "level": user.level,
        "xp_to_next": xp_to_next_level(user.xp),
        "solved": sorted(user.solved),
        "badges": sorted(user.badges),
    }


@router.get("/leaderboard")
def leaderboard(limit: int = 20):
    return {"leaderboard": store.leaderboard(limit)}


@router.get("/badges")
def badges():
    return {
        "badges": [
            {"id": key, "name": name, "description": desc, "emoji": emoji}
            for key, (name, desc, emoji) in BADGES.items()
        ]
    }
