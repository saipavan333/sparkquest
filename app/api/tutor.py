"""AI Tutor endpoint."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.catalog import get_catalog
from app.core.models import TutorRequest, TutorResponse
from app.tutor.tutor import get_reply

router = APIRouter(prefix="/api", tags=["tutor"])


@router.post("/tutor", response_model=TutorResponse)
def tutor(req: TutorRequest):
    challenge = get_catalog().get(req.challenge_id)
    if not challenge:
        raise HTTPException(status_code=404, detail="Challenge not found")
    reply, provider = get_reply(
        brief=challenge.brief,
        hints=challenge.hints,
        concepts=challenge.concepts,
        code=req.code,
        question=req.question,
        last_error=req.last_error,
    )
    return TutorResponse(reply=reply, provider=provider)
