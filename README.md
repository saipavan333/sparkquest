<div align="center">

# ⚡ SparkQuest

**Learn Python, PySpark & Spark Structured Streaming by playing — from zero to pro.**

A gamified, browser-based course where every challenge runs **real Spark** in a
sandbox and is graded automatically, with an AI tutor at your side.

[![CI](https://github.com/saipavan333/sparkquest/actions/workflows/ci.yml/badge.svg)](https://github.com/saipavan333/sparkquest/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org)
[![PySpark 3.5](https://img.shields.io/badge/PySpark-3.5.1-E25A1C.svg)](https://spark.apache.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-009688.svg)](https://fastapi.tiangolo.com)
[![Docker](https://img.shields.io/badge/Docker-ready-2496ED.svg)](Dockerfile)
[![🤗 Space](https://img.shields.io/badge/🤗%20Hugging%20Face-Live%20Demo-orange.svg)](https://huggingface.co/spaces/saipavan333/sparkquest)

[Live Demo](https://huggingface.co/spaces/saipavan333/sparkquest) ·
[Technical Paper](paper/sparkquest.pdf) ·
[Architecture](docs/ARCHITECTURE.md) ·
[Deployment](docs/DEPLOYMENT.md)

</div>

---

## Why SparkQuest?

Apache Spark is everywhere in data engineering, but beginners hit a wall: install a
JVM and Spark, fight `pip`, then wrestle with lazy evaluation before writing a
single useful line. SparkQuest removes that wall. You open a browser, write code,
hit **Run**, and a real Spark engine executes it — no setup. You earn XP, unlock
badges, and climb a leaderboard as you go from `print("hello")` to event-time
streaming with watermarks.

> **Not a quiz app.** Every submission is executed against an actual `SparkSession`
> and graded on its real output (DataFrames compared as multisets, not string
> matching).

## ✨ Features

- **Real execution, real feedback** — submissions run in a process-isolated sandbox
  with timeouts and output caps; grading inspects program state and DataFrame
  contents.
- **Zero → pro curriculum** — 16 finance-themed challenges across 3 tracks: Python
  Foundations → PySpark & Spark SQL → Structured Streaming.
- **AI tutor** — Socratic hints powered by your choice of Anthropic/OpenAI/HF, with
  a fully offline rule-based fallback so the demo is free.
- **Gamification** — XP, levels, badges (First Blood, Pythonista, Spark Wrangler,
  Stream Master…), and a live leaderboard.
- **In-browser IDE** — Monaco editor (the engine behind VS Code) with Run / Submit /
  Reset / Reveal-Solution.
- **Production-grade engineering** — typed FastAPI backend, pytest suite, Ruff lint,
  GitHub Actions CI, Docker, and one-command deploy to Hugging Face Spaces.
- **A real paper + real benchmarks** — reproducible PySpark benchmarks (optionally
  tracked in Weights & Biases) write the figures in [the paper](paper/sparkquest.pdf).

## 🏗️ Architecture

```
Browser (Monaco editor + SPA)
        │  JSON  /api/run  /api/submit  /api/tutor
        ▼
FastAPI app ──► YAML lesson catalog (solutions/checks hidden from client)
        │  job spec (code, checks, limits)
        ▼
Executor ──► child process  (hard timeout · output caps · loopback Spark)
        │
        ▼
Harness  ──► tuned SparkSession (local[k]) ─► declarative auto-grader
        │  verdict (stdout, per-check results, timings)
        ▼
Grader ──► XP · levels · badges · leaderboard
```

Untrusted code only ever runs in the **child process**, never in the API process.
Full design and the production-hardening path (gVisor/nsjail isolation, a warm
Spark Connect pool) are in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## 🚀 Quickstart

### Run locally (Python)

```bash
git clone https://github.com/saipavan333/sparkquest.git
cd sparkquest
pip install -r requirements.txt          # needs Java 11/17 on your PATH for Spark
python -m app.main                       # → http://localhost:7860
```

### Run with Docker (no Java/Python setup needed)

```bash
docker compose up --build                # → http://localhost:7860
```

### Optional: enable the live AI tutor

```bash
export SQ_TUTOR_PROVIDER=anthropic
export SQ_TUTOR_MODEL=claude-3-5-haiku-latest
export SQ_TUTOR_API_KEY=sk-...
```

Without a key, the tutor uses its built-in offline hint engine.

## 📚 Curriculum

| Track | Challenges | You'll learn |
|-------|:---------:|--------------|
| **Python Foundations** | 6 | variables, lists, dicts, functions, and a word-count *MapReduce bridge* |
| **PySpark & Spark SQL** | 7 | DataFrames, select/filter, `withColumn`, group-by, joins, SQL views, window functions |
| **Structured Streaming** | 3 | file-source streams, stateful aggregation, event-time windows + watermarks |

Lessons are plain YAML in [`lessons/`](lessons/) — add your own without touching code.

## 📊 Benchmarks (2 vCPU / 3.8 GiB host)

| Metric | Result |
|--------|--------|
| Group-by aggregation (2 M rows) | **4.41 M rows/s** |
| Structured Streaming ingestion | **~11,989 events/s** |
| Auto-grade, Python challenge | **~1 ms** |
| Auto-grade, Spark challenge (end-to-end) | **~8.6 s** (3.5 s Spark cold start) |
| Reference solutions passing their grader | **16/16 (100%)** |

Reproduce: `python benchmarks/run_benchmarks.py --all`. Details and discussion in
[the paper](paper/sparkquest.pdf).

## 🧪 Tests & quality

```bash
make test          # full suite (incl. real Spark tests)
make test-fast     # fast lane, no Spark startup
make lint          # ruff
make validate      # every lesson's reference solution must pass its grader
```

CI runs lint + solution validation + the full test suite on every push
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)).

## ☁️ Deploy

One-command Hugging Face Space deploy, container publishing to GHCR, and Render/Fly
configs — see [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).

## 🗺️ Roadmap

- Warm Spark Connect pool to remove cold-start latency
- Persistent progress + accounts
- Randomized-input grading to deter hard-coding
- Delta Lake & shuffle-tuning tracks; a capstone ETL on a real Kaggle dataset

## 📄 License

MIT — see [LICENSE](LICENSE). Built by [U E Sai Pavan Vamshi Krishna](https://github.com/saipavan333).
