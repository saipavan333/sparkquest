# Resources — go to the source

This handbook distills the books and docs below. When you want more depth than a
chapter gives, these are where to dig. (Editions current as of 2025/2026 — check
for newer.)

## The essential books

| Book | Author(s) | Why it matters for you |
|------|-----------|------------------------|
| **Spark: The Definitive Guide** | Chambers & Zaharia | The canonical Spark book, by the creator. DataFrames, Catalyst, structured streaming, deployment. Start here. |
| **Learning Spark, 2nd Edition** | Damji, Wenig, Das, Lee | Modern (Spark 3.x) and concise; great on the DataFrame API and Structured Streaming. Free PDF from Databricks. |
| **High Performance Spark** | Karau & Warren | The tuning bible — shuffles, joins, skew, memory. Exactly the senior-interview material in [chapter 02](02-performance-tuning.md). |
| **Designing Data-Intensive Applications** | Martin Kleppmann | *The* system-design book for data engineers — replication, partitioning, consistency, stream vs batch, logs. Read it cover to cover; it's the spine of the system-design round. |
| **Fundamentals of Data Engineering** | Reis & Housley | The modern DE lifecycle, the lakehouse, orchestration, the "big picture" senior context. |
| **Streaming Systems** | Akidau, Chernyak, Lax | The deep theory of event time, windows, and **watermarks** — the rigorous version of [chapter 03](03-streaming-internals.md). |
| **The Data Warehouse Toolkit** | Kimball & Ross | Dimensional modeling — star schemas, fact/dimension design, **SCDs**. Still the standard for modeling questions. |
| **Fluent Python** | Luciano Ramalho | Deep, idiomatic Python — generators, decorators, data model, concurrency. Levels up [Part VI](README.md). |

## Official documentation (always the truth for your version)

- **Apache Spark** — <https://spark.apache.org/docs/latest/>
  - SQL/DataFrames guide · Performance Tuning · Structured Streaming · Configuration
    · **Web UI** (learn to read it) · RDD programming guide · Tuning guide.
- **PySpark API reference** — <https://spark.apache.org/docs/latest/api/python/index.html>
- **Delta Lake** — <https://docs.delta.io/latest/index.html>
- **Apache Kafka** — <https://kafka.apache.org/documentation/>
- **Apache Iceberg** / **Apache Hudi** — for the "Delta vs Iceberg vs Hudi"
  lakehouse comparison.

## Free courses & practice

- **Databricks Academy** — free self-paced Spark/Delta paths and the *Learning
  Spark* PDF.
- **Apache Spark examples** — the `examples/` directory in the Spark repo is a
  goldmine of idiomatic code.
- **SparkQuest itself** — the 54 graded lessons are your practice reps; this
  handbook is the theory. Do both.

## A 6-week interview plan (≈1–2 hrs/day)

1. **Week 1 — Python + SQL reps.** Python track + SQL window/CTE patterns. Read
   *Fluent Python* chapters on generators/decorators.
2. **Week 2 — Spark core.** PySpark track + [chapter 01](01-spark-architecture-and-execution.md).
   Be able to draw jobs→stages→tasks and explain Catalyst.
3. **Week 3 — Performance.** Performance track + [chapter 02](02-performance-tuning.md).
   Drill skew/shuffle/join/AQE until automatic. *High Performance Spark.*
4. **Week 4 — Streaming.** Streaming track + [chapter 03](03-streaming-internals.md).
   Watermarks, state, exactly-once. Skim *Streaming Systems*.
5. **Week 5 — Lakehouse + modeling + capstones.** Delta track, Capstone track,
   *Kimball* SCDs. Build and narrate an end-to-end pipeline.
6. **Week 6 — Interviews.** Drill the [question bank](interview-questions.md) aloud;
   do timed system-design ("design X pipeline") from the framework; mock with a
   peer. *Designing Data-Intensive Applications* for the design round.

> The goal isn't to memorize — it's to build the mental models so you can *reason*
> about a new problem live. That's what separates a 50 LPA+ offer from a "thanks
> for coming in."
