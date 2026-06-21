"""Pydantic models: domain objects (Challenge) and API request/response schemas."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

# XP awarded per difficulty tier (1 = trivial, 5 = hard).
_XP_BY_DIFFICULTY = {1: 50, 2: 75, 3: 100, 4: 150, 5: 200}


class Challenge(BaseModel):
    """A single playable coding challenge, loaded from a YAML file."""

    id: str
    track: str  # python | pyspark | streaming
    order: int = 0
    title: str
    difficulty: int = 1
    concepts: list[str] = Field(default_factory=list)
    brief: str = ""
    needs_spark: bool = False
    needs_delta: bool = False
    needs_iceberg: bool = False
    starter_code: str = ""
    solution_code: str = ""
    checks: list[dict[str, Any]] = Field(default_factory=list)
    hints: list[str] = Field(default_factory=list)

    @property
    def xp(self) -> int:
        return _XP_BY_DIFFICULTY.get(self.difficulty, 50)


class TrackMeta(BaseModel):
    id: str
    title: str
    subtitle: str = ""
    order: int = 0


# ---------------- API request / response models ----------------

class RunRequest(BaseModel):
    challenge_id: str
    code: str


class SubmitRequest(BaseModel):
    challenge_id: str
    code: str
    user_id: str = "anonymous"


class CheckResult(BaseModel):
    passed: bool
    message: str
    detail: str = ""


class RunResponse(BaseModel):
    ran: bool
    stdout: str
    stderr: str
    error: str | None = None
    traceback: str | None = None
    duration_ms: int
    spark_startup_ms: int = 0
    timed_out: bool = False


class SubmitResponse(BaseModel):
    passed: bool
    checks: list[CheckResult]
    stdout: str
    stderr: str
    error: str | None = None
    traceback: str | None = None
    duration_ms: int
    spark_startup_ms: int = 0
    xp_awarded: int = 0
    total_xp: int = 0
    level: int = 1
    new_badges: list[str] = Field(default_factory=list)
    timed_out: bool = False


class TutorRequest(BaseModel):
    challenge_id: str
    code: str = ""
    question: str = ""
    last_error: str = ""


class TutorResponse(BaseModel):
    reply: str
    provider: str
