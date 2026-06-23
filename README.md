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
- **Zero → pro curriculum** — 92 finance-themed challenges across 7 tracks (and
  growing): Python for DE → PySpark & DataFrames (incl. **RDD low-level API**) →
  Performance → Structured Streaming → Lakehouse/Delta → Apache Iceberg (incl.
  **time travel**, **hidden partitioning**) → Capstone ETL (incl. **SCD2**,
  **data-quality** & **incremental** patterns). Each lesson ships **read-along
  teaching notes**. Full [syllabus](docs/CURRICULUM.md).
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
- **Interview-grade handbook + Mock Interview** — **15 book-quality deep-dive
  chapters** (architecture, performance, streaming, file formats, lakehouse,
  modeling, system design, Python/SQL, **joins & AQE**, **RDDs**, **config &
  cluster sizing**, **debugging & the Spark UI**, **Kafka**, **Iceberg**, **testing
  & data quality**), readable **in-app**, plus a **🎤 Mock Interview** drill over a
  **201-question bank** with self-scoring and a **timed, scored exam mode** ([docs/handbook](docs/handbook/)).

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

The full syllabus — every topic with official-doc links — lives in
**[docs/CURRICULUM.md](docs/CURRICULUM.md)**. Seven ordered tracks:

| Track | Live | You'll learn |
|-------|:----:|--------------|
| **Python for Data Engineering** | 27 | **data types & conversion**, **strings & slicing**, **conditionals**, variables, lists, dicts, **enumerate/zip**, comprehensions, functions, **lambdas & functional style**, errors, generators, decorators, dataclasses, sets/tuples, collections, datetime, json, **regex**, **context managers**, **type hints**, **pathlib**, **CSV**, **logging** |
| **PySpark Foundations & DataFrames** | 36 | schemas, Parquet, select/filter, **sorting**, **distinct/dedup**, when/otherwise, string/date/null/cast, all join types, **windows (rank/dense_rank/lag/lead/running totals)**, set ops, maps/structs, pivot, `explode`, UDFs, selectExpr, **sessionization**, **pandas UDFs**, **RDDs (map/filter, reduceByKey, broadcast vars, mapPartitions)**, **Spark SQL analytics**, **data-quality validation** |
| **Performance & Internals** | 7 | repartition/coalesce, caching, broadcast joins, data skew & salting, partitioned writes & **partition pruning**, **bucketing** |
| **Structured Streaming** | 8 | sources/sinks, aggregations, event-time windows, watermarks, dedup, `foreachBatch`, stream-static **and stream-stream joins**, **Kafka value parsing (`from_json`)** |
| **Lakehouse & Delta Lake** | 4 | create Delta tables, `MERGE`/upsert (CDC), time travel, schema evolution |
| **Apache Iceberg** | 5 | create Iceberg tables, row-level `UPDATE`/`DELETE` (copy-on-write), **`MERGE`/upsert**, **time travel**, **hidden partitioning** |
| **Capstone ETL** | 5 | end-to-end batch ETL, data-quality checks, a live streaming pipeline, **SCD Type 2**, **incremental/high-water-mark** |

Lessons are plain YAML in [`lessons/`](lessons/) — add your own without touching code.

## 📖 The Handbook — interview-grade depth

The lessons build muscle memory; the **[SparkQuest Handbook](docs/handbook/)** builds
the mental models senior data-engineering interviews probe — **15 deep-dive
chapters** spanning
[architecture & execution](docs/handbook/01-spark-architecture-and-execution.md),
[performance tuning](docs/handbook/02-performance-tuning.md),
[Structured Streaming internals](docs/handbook/03-streaming-internals.md),
[joins, shuffle & AQE](docs/handbook/09-joins-shuffle-aqe.md),
[RDDs & the low-level API](docs/handbook/10-rdd-and-low-level-api.md),
[configuration & cluster sizing](docs/handbook/11-configuration-and-cluster-sizing.md),
[debugging & the Spark UI](docs/handbook/12-debugging-and-spark-ui.md),
[Kafka & streaming I/O](docs/handbook/13-kafka-and-streaming-io.md),
[Apache Iceberg](docs/handbook/14-apache-iceberg.md), and
[testing & data quality](docs/handbook/15-testing-and-data-quality.md) — plus an
[interview question bank](docs/handbook/interview-questions.md) with model answers and
a curated [reading list & 6-week study plan](docs/handbook/resources.md). Grounded in
the official Spark docs and the canonical books.

## 📊 Benchmarks (2 vCPU / 3.8 GiB host)

| Metric | Result |
|--------|--------|
| Group-by aggregation (2 M rows) | **4.41 M rows/s** |
| Structured Streaming ingestion | **~11,989 events/s** |
| Auto-grade, Python challenge | **~1 ms** |
| Auto-grade, Spark challenge (end-to-end) | **~8.6 s** (3.5 s Spark cold start) |
| Reference solutions passing their grader | **83/83 sandbox + 9 Delta & Iceberg (CI)** |

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
- Expand the Apache Iceberg track (time travel, hidden partitioning, MERGE)

## 📄 License

MIT — see [LICENSE](LICENSE). Built by [U E Sai Pavan Vamshi Krishna](https://github.com/saipavan333).
