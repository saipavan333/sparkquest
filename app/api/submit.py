"""Code execution endpoints: run (ungraded) and submit (graded, awards XP)."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.catalog import get_catalog
from app.core import grader
from app.core.gamification import store
from app.core.models import (
    CheckResult,
    RunRequest,
    RunResponse,
    SubmitRequest,
    SubmitResponse,
)

router = APIRouter(prefix="/api", tags=["execution"])


@router.post("/run", response_model=RunResponse)
def run(req: RunRequest):
    challenge = get_catalog().get(req.challenge_id)
    if not challenge:
        raise HTTPException(status_code=404, detail="Challenge not found")
    res = grader.run_only(challenge, req.code)
    return RunResponse(
        ran=res.ran,
        stdout=res.stdout,
        stderr=res.stderr,
        error=res.error,
        traceback=res.traceback,
        duration_ms=res.duration_ms,
        spark_startup_ms=res.spark_startup_ms,
        timed_out=res.timed_out,
    )


@router.post("/submit", response_model=SubmitResponse)
def submit(req: SubmitRequest):
    catalog = get_catalog()
    challenge = catalog.get(req.challenge_id)
    if not challenge:
        raise HTTPException(status_code=404, detail="Challenge not found")

    result = grader.grade(challenge, req.code)

    user = store.get(req.user_id)
    total_xp, level, new_badges = user.xp, user.level, []
    if result.passed:
        total_xp, level, new_badges = store.record_solve(
            req.user_id,
            challenge.id,
            result.xp_awarded,
            catalog.by_track(),
            catalog.all_ids(),
        )

    return SubmitResponse(
        passed=result.passed,
        checks=[CheckResult(**chk) for chk in result.checks],
        stdout=result.stdout,
        stderr=result.stderr,
        error=result.error,
        traceback=result.traceback,
        duration_ms=result.duration_ms,
        spark_startup_ms=result.spark_startup_ms,
        xp_awarded=result.xp_awarded,
        total_xp=total_xp,
        level=level,
        new_badges=new_badges,
        timed_out=result.timed_out,
    )
