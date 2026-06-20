# 05 · Lakehouse & Delta Lake

> *Interview lens: "What does Delta give you that plain Parquet doesn't?" Then:
> "How does the transaction log work?", "OPTIMIZE vs VACUUM?", "Delta vs Iceberg?"
> The lakehouse is the dominant modern architecture — own this.*

## 1. Why a lakehouse exists

A **data lake** (Parquet on S3/ADLS/GCS) is cheap and scalable but **dumb files**:
no transactions, so a failed write leaves half-written data; no upserts, so you
can't do CDC; concurrent writers corrupt each other; no schema enforcement; no
"what did it look like yesterday." A **data warehouse** has all that but is
expensive and closed.

The **lakehouse** = warehouse guarantees **on** lake storage, via a **table
format** (Delta Lake, Apache Iceberg, Apache Hudi) that adds a **transaction log**
over the Parquet files. You get ACID, upserts, time travel, and schema management
while keeping open files and cheap object storage.

## 2. The Delta transaction log (the heart of it)

A Delta table is **Parquet data files + a `_delta_log/` directory**:

```
/table/
  part-0001.parquet, part-0002.parquet, ...
  _delta_log/
    00000000000000000000.json   ← commit 0 (atomic: list of add/remove actions)
    00000000000000000001.json   ← commit 1
    ...
    00000000000000000010.checkpoint.parquet  ← every 10 commits, a snapshot
```

- Each **commit** is an atomic JSON file listing **actions**: `add` (a new data
  file + its stats), `remove` (tombstone an old file), `metaData` (schema),
  `commitInfo`. The table's current state = replay all commits (or read the latest
  **checkpoint** + the few commits after it — that's why checkpoints exist:
  bounded read cost).
- **Atomicity** comes from the log: a writer stages new Parquet files, then
  *atomically* writes commit N. Until that JSON lands, readers see version N-1.
  Readers never see partial data.
- **Optimistic concurrency control** — two writers both try to write commit N; the
  log's atomic put means only one wins; the loser **re-reads** and retries against
  N (or fails if there's a true conflict). This is how Delta does serializable
  isolation without locks.
- **Reads** are snapshot-consistent: a query pins a version, reads exactly those
  files, ignores in-flight writes.

This log is also what powers **time travel** (`versionAsOf` / `timestampAsOf` — just
read the file set as of commit K) and **`MERGE`** (read matching files, write new
ones, commit the add/remove set atomically).

## 3. The operations a senior must know

- **`MERGE INTO`** — atomic upsert/CDC; the killer feature (lesson `delta-02`).
- **Time travel** — `versionAsOf`/`timestampAsOf` for audits, debugging, repro
  (lesson `delta-03`).
- **Schema enforcement & evolution** — Delta *rejects* writes that don't match the
  schema (no silent corruption); opt into changes with `mergeSchema` (lesson
  `delta-04`).
- **`OPTIMIZE`** — **compaction**: rewrites many small files into few right-sized
  ones (fixes the small-files problem from incremental/streaming writes).
- **`OPTIMIZE ... ZORDER BY (col)`** — **Z-ordering**: co-locates rows with similar
  values of high-cardinality filter columns into the same files, so predicate
  pushdown skips far more files. Use on the columns you filter/join on but can't
  partition by.
- **`VACUUM`** — physically **deletes** old, tombstoned data files no longer
  referenced (after a retention window, default 7 days). `OPTIMIZE` *creates* new
  files and tombstones old; `VACUUM` *removes* the tombstoned ones. Running
  `VACUUM` too aggressively **breaks time travel** (the old files are gone).
- **Change Data Feed (CDF)** — Delta can emit the row-level changes
  (insert/update/delete) of each version, so downstream consumers process only what
  changed — native CDC out of a Delta table.
- **Deletion vectors** — mark rows deleted/updated *without* rewriting whole files
  (merge-on-read), making deletes/updates cheap; the rewrite happens later at
  `OPTIMIZE`.

**OPTIMIZE vs VACUUM** (the classic trap): OPTIMIZE = *compact small files into
big ones* (perf); VACUUM = *garbage-collect unreferenced old files* (storage).
Different jobs.

## 4. Delta vs Iceberg vs Hudi

All three are open table formats giving ACID + time travel + schema evolution over
Parquet. Differences in emphasis (verify current state — they converge fast):

| | **Delta Lake** | **Apache Iceberg** | **Apache Hudi** |
|---|---|---|---|
| Origin | Databricks | Netflix | Uber |
| Metadata | `_delta_log` (JSON + checkpoints) | manifest-list/manifest tree + snapshots | timeline + file groups |
| Strength | tight Spark/Databricks integration, `MERGE`, simplicity | engine-agnostic, **hidden partitioning**, partition evolution, huge-table metadata scaling | **fast upserts/CDC**, record-level indexes, MoR vs CoW |
| Write model | copy-on-write (+ deletion vectors) | copy-on-write & merge-on-read | **copy-on-write or merge-on-read** |

Interview-safe summary: *"All three solve the same problem — ACID tables on a lake.
Delta is simplest and Spark-native; Iceberg is the most engine-agnostic with strong
partition evolution and metadata scaling; Hudi pioneered low-latency upserts/CDC.
Pick by your engine ecosystem and whether you need heavy upserts."*

## 5. Medallion architecture (the standard pipeline shape)

Organize a lakehouse into three quality tiers:

- **Bronze** — raw, append-only ingestion (schema-on-read, keep everything,
  including bad data; your replayable source of truth).
- **Silver** — cleaned, conformed, deduplicated, joined to reference data;
  business entities. This is where data-quality gates and `MERGE`/CDC live.
- **Gold** — aggregated, business-level marts/features ready for BI/ML.

Each layer is a Delta table; you move data with idempotent `MERGE`s so re-runs are
safe. This is the architecture to **draw** in a system-design round (see
[chapter 07](07-system-design.md)).

---

**Next:** [06 · Data Modeling](06-data-modeling.md) — how to *shape* those gold
tables.
