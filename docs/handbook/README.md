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
   out loud; an interviewer wants reasoning, not recitation.
4. **Build the capstones** and be ready to whiteboard a pipeline end-to-end.

Every claim here is grounded in the official Apache Spark docs and the canonical
books listed in [resources.md](resources.md). Spark evolves — verify version-
specific configs against the docs for *your* runtime.

## The senior syllabus

### Part I — Distributed execution (the "how Spark really works" layer)
- **[01 · Architecture & Execution](01-spark-architecture-and-execution.md)** —
  driver/executors, jobs→stages→tasks, the DAG, lazy evaluation, RDD vs
  DataFrame vs Dataset, Catalyst's four phases, Tungsten & whole-stage codegen.
- **The shuffle** — why wide transformations cost so much, map/reduce sides,
  shuffle files, sort vs hash, the 200-partition default. *(in 01 & 02)*
- **Join strategies** — broadcast hash, sort-merge, shuffle hash, broadcast
  nested loop; how Spark picks; how to force one. *(in 02)*

### Part II — Performance engineering (the senior differentiator)
- **[02 · Performance Tuning & Debugging](02-performance-tuning.md)** —
  partition sizing, caching, broadcast joins, **data skew** (salting & AQE skew
  join), **spill & OOM**, executor/core/memory sizing, dynamic allocation,
  Adaptive Query Execution, and **reading the Spark UI** like a detective.

### Part III — Real-time systems
- **[03 · Structured Streaming Internals](03-streaming-internals.md)** —
  micro-batch vs continuous, the unbounded-table model, **event time vs
  processing time**, **watermarks**, the **state store** (HDFS vs RocksDB),
  **checkpointing**, **exactly-once** via idempotent/transactional sinks, output
  modes, triggers, stream-stream joins, and Kafka integration.

### Part IV — Storage, modeling & the lakehouse *(expanding)*
- File formats deep-dive — row vs columnar, **Parquet** internals (row groups,
  column chunks, pages, encodings, predicate/column pushdown), ORC, Avro.
- **Partitioning vs bucketing** — write-time layout, the small-files problem,
  compaction, file sizing (~128 MB–1 GB targets).
- **Delta Lake deep** — transaction log, `OPTIMIZE` + Z-order, `VACUUM`,
  Change Data Feed, deletion vectors; **Delta vs Iceberg vs Hudi**.
- **Dimensional modeling** — star/snowflake schemas, fact/dimension tables,
  **Slowly Changing Dimensions (SCD types 1/2/3)**, normalization tradeoffs.

### Part V — Pipeline & system design
- **Batch vs streaming**, **Lambda vs Kappa** architectures.
- **Idempotency, exactly-once, and backfills** — designing for re-runs.
- **Data quality** — contracts, expectations, quarantine, observability.
- **Orchestration** — DAGs, dependencies, retries, SLAs (Airflow mental model).
- **The data-pipeline system-design interview** — a repeatable framework.

### Part VI — Python & SQL mastery for DE
- **Python deep** — generators/iterators, decorators, context managers,
  **concurrency** (threading vs multiprocessing vs asyncio, the GIL), memory,
  typing, testing, packaging.
- **SQL deep** — window functions, CTEs & recursion, advanced aggregation,
  query-plan reading, the classic interview SQL patterns.

### Part VII — Interview craft
- **[Interview Question Bank](interview-questions.md)** — conceptual Spark,
  PySpark/SQL coding, system design, and Python, each with a model answer.
- **[Resources](resources.md)** — the books, courses, and docs that this
  handbook distills, so you can go to the source.

## Status & rollout

This handbook is being written in phases alongside the lessons. **Live now:**
Parts I–III (architecture, performance, streaming), the question bank, and
resources. **Next:** Parts IV–VI as full chapters (the syllabus above is the
contract for what's coming). The lessons already cover the *hands-on* side of
Parts IV–VI; the handbook adds the depth an interviewer will dig for.
