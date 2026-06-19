# End-to-End Blueprint

The full playbook used to take SparkQuest from idea to a live, portfolio-grade
project — reusable for your next one.

## 0. Concept & positioning

- **Idea:** a gamified, browser-based course that teaches Python → PySpark → Spark
  Structured Streaming, executing real Spark and grading automatically.
- **Why it's a strong portfolio piece (esp. for data-engineering / fintech roles):**
  it demonstrates distributed data processing, streaming, sandboxed execution,
  clean API design, testing, containerization, CI/CD, and cloud deployment — the
  exact competencies those teams hire for — wrapped in something recruiters can
  *click and try*.

## 1. Architecture first

Decide the seams before coding (see [ARCHITECTURE.md](ARCHITECTURE.md)):
backend (FastAPI) · execution sandbox (subprocess + harness) · grader (declarative
checks) · content (YAML) · frontend (Monaco SPA) · gamification (pluggable store).
Designing the `build_spark_session` factory and the executor boundary up front is
what makes later hardening (Spark Connect, container isolation) a swap, not a
rewrite.

## 2. Build the engine, validate continuously

1. Backend: typed models, config, Spark session factory, harness, executor, grader,
   gamification, tutor, API routes.
2. **Validate the riskiest path early** — a real Python *and* a real PySpark *and* a
   streaming submission through the sandbox — before writing all content. (This is
   where we found and fixed the Spark hostname-resolution issue with
   `SPARK_LOCAL_IP`.)

## 3. Author content as data

Lessons are YAML (`lessons/<track>/*.yaml`) with brief, starter code, reference
solution, grader checks, and hints. Keep challenges deterministic (build small
DataFrames inline; stream from bundled files with `Trigger.AvailableNow`).
**Gate:** every reference solution must pass its own grader
(`python scripts/validate_lessons.py`).

## 4. Frontend

A dependency-light SPA: Monaco editor, lesson navigator, Run/Submit, console,
gamification, and an AI tutor panel. Served as static files by FastAPI (one
container, one origin).

## 5. Test & lint

- Unit tests for the sandbox (incl. timeout), API tests via `TestClient`, and a
  parameterized test that grades **every** reference solution (Spark tests marked
  `spark` for a fast/full CI split).
- `ruff` for linting/formatting.

## 6. Containerize

`Dockerfile` mirrors the validated runtime (Python 3.10 + JRE 11 + PySpark 3.5.1),
runs as a non-root user on port 7860 (Hugging Face Spaces convention), with a
healthcheck. `docker compose up --build` is the one-command local run.

## 7. CI/CD

GitHub Actions:
- **CI** — lint, validate solutions, run tests on every push/PR.
- **Publish** — build & push the image to GHCR on `main`/tags.
- **Deploy** — sync to a Hugging Face Docker Space (no-ops without the `HF_TOKEN`
  secret, so forks don't fail).

## 8. Benchmark & write it up

A reproducible benchmark harness (`benchmarks/run_benchmarks.py`, optional W&B
logging) produces real numbers and the paper's figures. The paper
(`paper/sparkquest.tex` → PDF) documents design, security, grading, and results in
an IEEE-style format.

## 9. Publish & share

Push to GitHub (public), create the `ai-projects` branch, set the repo description +
topics, deploy the Space, and post on LinkedIn — exact commands and copy in
[DEPLOYMENT.md](DEPLOYMENT.md) and [LINKEDIN_POST.md](LINKEDIN_POST.md).

## Account map (what each tool is for here)

| Account | Role in this project |
|---------|----------------------|
| **GitHub** | Source, CI/CD, container registry (GHCR), public portfolio home |
| **Docker** | Containerization; optional Docker Hub publishing |
| **Hugging Face** | Free live demo (Docker Space) + optional tutor model hosting |
| **Weights & Biases** | Experiment tracking for the benchmark sweeps |
| **Kaggle** | Optional real dataset for scaled benchmarks / a capstone lesson |
| **VS Code** | Local development |
