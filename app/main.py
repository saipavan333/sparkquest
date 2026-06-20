"""SparkQuest FastAPI application entrypoint."""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app import __version__
from app.api import handbook, lessons, progress, submit, tutor
from app.catalog import get_catalog
from app.config import get_settings

STATIC_DIR = Path(__file__).resolve().parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load (and validate) the catalog at startup so YAML errors fail fast.
    catalog = get_catalog()
    app.state.challenge_count = len(catalog.challenges)
    yield


app = FastAPI(
    title="SparkQuest",
    description="Learn Python, PySpark, and Spark Structured Streaming by playing.",
    version=__version__,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(lessons.router)
app.include_router(submit.router)
app.include_router(tutor.router)
app.include_router(progress.router)
app.include_router(handbook.router)


@app.get("/healthz", tags=["meta"])
def healthz():
    return {"status": "ok", "version": __version__, "challenges": len(get_catalog().challenges)}


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_DIR / "index.html")


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=not settings.is_production,
    )


if __name__ == "__main__":
    main()
