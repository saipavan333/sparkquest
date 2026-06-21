# The SparkQuest Handbook — a senior data engineer's interview bible

> The deep-knowledge companion to the interactive lessons. The lessons build
> *muscle memory*; this handbook builds the *mental models and judgment* that
> distinguish a senior data engineer in interviews at top-tier companies.

If you can write the code (the 54 graded lessons) **and** explain everything in
this handbook in your own words, you are ready for the L4–L6 / SDE-II–III data
engineering loop at FAANG-tier and high-paying product companies.

## How to use this

1. **Do the lessons** (interactive app) until the syntax is automatic.
2. **Read each deep-dive** and re-derive the diagrams from memory.
3. **Drill the [interview question bank](interview-questions.md)** — say answers
   out loud; an interviewer wants reasoning, not recitation. Or fire up the in-app
   **🎤 Mock Interview** for randomized, self-scored drilling over a **201-question
   bank**, with a **timed, scored exam mode** when you want interview-day pressure.
4. **Build the capstones** and be ready to whiteboard a pipeline end-to-end.

Every claim here is grounded in the official Apache Spark docs and the canonical
books listed in [resources.md](resources.md). Spark evolves — verify version-
specific configs against the docs for *your* runtime.

## The senior syllabus

### Part I — Distributed execution (the "how Spark really works" layer)
- **[01 · Architecture & Execution](01-spark-architecture-and-execution.md)** —
  driver/executors, jobs→stages→tasks, the DAG, lazy evaluation, RDD vs
  DataFrame vs Dataset, Catalyst's four phases, Tungsten & whole-stage codegen.
- **[09 · Joins, Shuffle & AQE Deep-Dive](09-joins-shuffle-aqe.md)** — the five
  join strategies and exactly when Spark picks each, the shuffle in detail,
  reading a physical plan, **Adaptive Query Execution** (coalesce, skew join,
  runtime broadcast), and de-skewing with salting/broadcast.
- **[10 · RDDs & the Low-Level API](10-rdd-and-low-level-api.md)** — the five RDD
  properties, lineage, `reduceByKey` vs `groupByKey`, persistence/storage levels,
  **broadcast variables & accumulators**, and when (not) to drop below DataFrames.

### Part II — Performance engineering (the senior differentiator)
- **[02 · Performance Tuning & Debugging](02-performance-tuning.md)** —
  partition sizing, caching, broadcast joins, **data skew** (salting & AQE skew
  join), **spill & OOM**, executor/core/memory sizing, dynamic allocation,
  Adaptive Query Execution, and **reading the Spark UI** like a detective.
- **[11 · Configuration & Cluster Sizing](11-configuration-and-cluster-sizing.md)** —
  deploy modes & cluster managers, the executors×cores×memory math (worked
  example), the unified memory model, parallelism, dynamic allocation, and the
  config reference table.
- **[12 · Debugging & the Spark UI](12-debugging-and-spark-ui.md)** — the six UI
  tabs, reading a stage's task distribution for **skew/spill/GC**, a repeatable
  triage playbook, and a table of common exceptions with fixes.

### Part III — Real-time systems
- **[03 · Structured Streaming Internals](03-streaming-internals.md)** —
  micro-batch vs continuous, the unbounded-table model, **event time vs
  processing time**, **watermarks**, the **state store** (HDFS vs RocksDB),
  **checkpointing**, **exactly-once** via idempotent/transactional sinks, output
  modes, triggers, stream-stream joins.
- **[13 · Kafka & Streaming I/O](13-kafka-and-streaming-io.md)** — Kafka in 90
  seconds, the fixed source schema, parsing `value` with `from_json`, writing to
  Kafka, offset/flow control, **where exactly-once actually comes from**, and the
  `foreachBatch`+Delta `MERGE` production sink.

### Part IV — Storage, modeling & the lakehouse
- **[04 · File Formats & Physical Layout](04-file-formats-and-layout.md)** — row vs
  columnar, **Parquet** internals (row groups, pages, encodings, predicate/column
  pushdown), partitioning vs bucketing, the small-files problem.
- **[05 · Lakehouse & Delta Lake](05-lakehouse-and-delta.md)** — the transaction
  log, ACID, `MERGE`, `OPTIMIZE`+Z-order, `VACUUM`, CDF; **Delta vs Iceberg vs
  Hudi**; medallion architecture.
- **[06 · Data Modeling](06-data-modeling.md)** — star/snowflake schemas,
  fact/dimension design, **Slowly Changing Dimensions (SCD 1/2/3)**.
- **[14 · Apache Iceberg](14-apache-iceberg.md)** — the metadata/snapshot/manifest
  tree, **hidden partitioning** & partition evolution, time travel, **copy-on-write
  vs merge-on-read**, table maintenance, and **Delta vs Iceberg vs Hudi**.

### Part V — Pipeline & system design
- **[07 · Pipeline System Design](07-system-design.md)** — a repeatable framework;
  batch vs streaming; **Lambda vs Kappa**; **idempotency, exactly-once & backfills**;
  data quality; orchestration (Airflow).

### Part VI — Python & SQL mastery for DE
- **[08 · Python & SQL Mastery](08-python-and-sql-deep.md)** — the GIL & concurrency
  models, generators, decorators, typing, testing; SQL **window functions**, CTEs,
  advanced aggregation, the classic interview patterns, and query optimization.

### Part VII — Interview craft
- **[Interview Question Bank](interview-questions.md)** — conceptual Spark,
  PySpark/SQL coding, system design, and Python, each with a model answer.
- **[Resources](resources.md)** — the books, courses, and docs that this
  handbook distills, so you can go to the source.

## Status & rollout

**All 14 deep-dive chapters are live** (Parts I–VI) plus the question bank and
resources — and they're readable **inside the app** via the 📖 **Handbook** button,
not just on GitHub. A **🎤 Mock Interview** mode drills you through a **201-question
bank** (Spark core, performance, streaming, Kafka, file formats, lakehouse, modeling,
SQL, Python, system design, orchestration, coding, scenarios, behavioral) with
self-scoring — and a **timed, scored exam mode** — also in the app. The lessons give
the hands-on reps; the handbook gives the depth an interviewer digs for. It keeps
growing — open an issue with the topic you want next.
