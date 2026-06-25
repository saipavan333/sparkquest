# 07 · Pipeline System Design

> *Interview lens: the senior round. They don't want code — they want to see you
> reason about tradeoffs under real constraints. Use one repeatable framework
> every time so you never freeze.*

> **In plain words.** This chapter zooms out from single commands to designing a
> whole pipeline: where data comes in, how it flows through cleaning and
> transforming, and where it lands — plus the practical worries like "what if it
> fails halfway?" and "how do we re-run yesterday safely?". It gives you a simple,
> repeatable way to think through any "design a data pipeline" question.

## 1. The framework (memorize the spine)

**Requirements → Data model → Ingestion → Processing → Storage → Serving →
Quality & Ops → Tradeoffs.** Walk it top to bottom, out loud, and *clarify before
you build*.

### Step 1 — Clarify requirements (always first)
Ask, don't assume:
- **Scale** — events/day, bytes/day, growth.
- **Latency / freshness** — real-time (seconds), near-real-time (minutes), or daily
  batch? This single answer drives batch-vs-streaming.
- **SLA** — when must data be ready? How bad is being late vs being wrong?
- **Consistency** — exact counts or are approximations OK?
- **Access pattern** — who/what queries it, and how (point lookups, scans, BI)?
- **Budget** — cost sensitivity shapes everything.

> "Before I design, let me confirm scale, freshness, and whether we need exact or
> approximate results" — *start every system-design answer this way.*

## 2. Batch vs streaming (the central fork)

- **Batch** when freshness in hours/days is fine: simpler, cheaper, easier to
  reprocess, exact. Most "daily metrics" are batch (or incremental batch with
  `availableNow`).
- **Streaming** when you need seconds–minutes latency (fraud, monitoring, live
  dashboards): more complex (state, watermarks, exactly-once), pricier.
- Default to **batch/incremental** unless latency demands streaming. Saying
  "everything streaming" is a junior tell.

### Lambda vs Kappa
- **Lambda** — a **batch layer** (accurate, slow, source of truth) *plus* a **speed
  layer** (approximate, fast), merged at serving. Robust but **two codebases** to
  maintain and reconcile.
- **Kappa** — a **single streaming pipeline**; reprocess history by **replaying the
  log** (Kafka/Delta). One codebase. Modern lakehouses (streaming + Delta) make
  Kappa-style increasingly viable.
- Answer: *"Kappa if a single streaming path can also serve the batch truth via
  replay; Lambda if you genuinely need a separate, simpler batch truth layer."*

## 3. The reference shape (draw this)

```
Sources → Kafka (ingest/buffer) → Spark (Structured Streaming or incremental batch)
        → Bronze (raw Delta) → Silver (clean/dedup/join) → Gold (aggregates)
        → Warehouse / serving DB / feature store → BI / API / ML
                ↑ orchestration (Airflow)   ↑ data-quality gates   ↑ monitoring
```

This **medallion + Kafka + orchestration** picture answers 80% of "design a data
pipeline for X" prompts. Adapt the processing (stream vs batch) and serving layer
to the requirements.

## 4. Idempotency, exactly-once & backfills (where seniors shine)

Pipelines **will** re-run (retries, backfills, replays). Design so a re-run produces
the **same** result — *idempotency* — or you get double-counts.

Techniques:
- **Upserts keyed by a deterministic business/event id** (Delta `MERGE`) instead of
  blind `append` — re-processing the same event overwrites, doesn't duplicate.
- **Partition overwrite** — process one date into `dt=...`; a re-run **replaces
  exactly that partition** (`replaceWhere` / dynamic partition overwrite). Backfill
  = "delete the date, re-run."
- **Record processed offsets/files** — streaming checkpoints, or a control table of
  processed batches, so you never reprocess silently.
- **Deterministic transforms** — no `now()`/random in keys; same input → same
  output.

**Backfill** = re-run history (new logic, fixed bug, late data). A well-designed
pipeline makes it boring: idempotent writes + date-partitioned + parameterized by
run-date.

## 5. Data quality (a whole sub-topic now)

Bad data silently breaks dashboards and models. Build **gates**, not hope:
- **Schema/contracts** — enforce schema on write (Delta does); reject drift.
- **Expectations** — row counts in expected range, null rate < X%, values in a
  set/range, uniqueness of keys, referential integrity. (Tools: Great
  Expectations, dbt tests, Deequ; or hand-rolled assertions like SparkQuest's
  `cap-02`.)
- **Quarantine** — route failing rows to a side table instead of dropping or
  crashing; alert and review.
- **Observability** — freshness, volume, distribution drift, and lineage
  (data-downtime monitoring).

## 6. Orchestration (the Airflow mental model)

A pipeline is a **DAG** of tasks with dependencies. The orchestrator (Airflow,
Dagster, …) provides:
- **Scheduling** (cron / data-aware), **dependencies** (run B after A),
- **Retries with backoff**, **SLAs/alerts**, **backfills** (run for a date range),
- **Idempotent, parameterized tasks** (each task keyed by `execution_date` so
  re-runs are safe — ties back to §4).

Say: *"Tasks must be idempotent and parameterized by run-date so the orchestrator
can retry and backfill safely."*

## 7. A worked example — "design DAU from clickstream"

1. **Clarify:** ~1 B events/day, daily freshness OK, exact-ish DAU, BI consumers.
2. **Ingest:** app → **Kafka** (buffer, replayable).
3. **Process:** daily **incremental batch** (Spark, `availableNow`) — latency is
   daily, so no streaming complexity.
4. **Storage (medallion):** **bronze** raw events (Delta, partitioned by `dt`); 
   **silver** dedup + sessionize; **gold** = distinct users per `dt`.
5. **Late events:** a look-back window (reprocess last N days) with **idempotent
   `MERGE`** so re-counting a day doesn't double up.
6. **Exactness vs cost:** exact `COUNT(DISTINCT user)` is a big shuffle; if
   approximate is OK, **HyperLogLog** (`approx_count_distinct`) is far cheaper and
   mergeable across days.
7. **Quality:** assert daily row counts and null rates; quarantine malformed events.
8. **Ops:** Airflow DAG, partition-overwrite writes (safe backfills), freshness
   alerts.
9. **Tradeoffs:** batch (simple/cheap) over streaming (DAU doesn't need seconds);
   HLL (cheap, ~2% error) vs exact (costly) — *call out the tradeoff and let the
   interviewer steer.*

> The grader can't test this chapter — but `cap-01`/`cap-02`/`cap-03` let you
> *build* the ingest→clean→aggregate→quality spine you'll describe here.

---

**Next:** [08 · Python & SQL Mastery](08-python-and-sql-deep.md).
