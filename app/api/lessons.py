"""Lesson catalog endpoints."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.catalog import get_catalog

router = APIRouter(prefix="/api", tags=["lessons"])


@router.get("/tracks")
def get_tracks():
    return {"tracks": get_catalog().public_tracks()}


@router.get("/challenge/{challenge_id}")
def get_challenge(challenge_id: str):
    data = get_catalog().public_challenge(challenge_id)
    if not data:
        raise HTTPException(status_code=404, detail="Challenge not found")
    return data


@router.get("/challenge/{challenge_id}/solution")
def get_solution(challenge_id: str):
    challenge = get_catalog().get(challenge_id)
    if not challenge:
        raise HTTPException(status_code=404, detail="Challenge not found")
    return {"id": challenge.id, "solution_code": challenge.solution_code}
