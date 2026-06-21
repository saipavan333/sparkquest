# 12 · Debugging & the Spark UI

> *Interview lens: "A job that used to take 10 minutes now takes 2 hours. Walk me
> through how you'd debug it." The answer is a **method**, not a guess — and it
> starts in the Spark UI.*
>
> Source of truth: the official
> [Web UI guide](https://spark.apache.org/docs/3.5.1/web-ui.html) and
> [Tuning guide](https://spark.apache.org/docs/3.5.1/tuning.html).

## 1. The Spark UI tabs (where to look)

Reachable at `http://<driver>:4040` while the app runs (or the History Server
after). Six tabs matter:

- **Jobs** — one row per action. Long-running or failed jobs first.
- **Stages** — the workhorse. Each stage shows the **task time distribution**,
  shuffle read/write, input/output, and **spill**. This is where you diagnose.
- **Storage** — what's cached, its size, and fraction in memory vs spilled.
- **Environment** — every effective config (confirm your settings actually took).
- **Executors** — per-executor cores, memory used, **GC time**, failed tasks,
  shuffle. Spot a dying or skewed executor here.
- **SQL / DataFrame** — the query plan as a **DAG** with per-operator row counts
  and time; click through to see `Exchange`, join types, and pushed filters.

## 2. Reading a stage like a pro

Open a slow stage → the **task metrics summary** (min · 25th · median · 75th ·
max). The shape tells you the disease:

- **`max` duration ≫ `median`** → **skew**: one task got a huge partition. Also
  shows as one task's **shuffle read** dwarfing the rest.
- **Spill (memory)** and **Spill (disk)** non-zero → **memory pressure**: the
  partition didn't fit, so Spark spilled. Fix with more/smaller partitions, more
  memory, or less data per task.
- **High GC time** (Executors tab) → heap churn; reduce caching, use Kryo, or
  raise memory.
- **Huge `Shuffle Read`/`Write`** → an expensive wide transformation; can you
  broadcast, pre-aggregate, or prune earlier?
- **Many tiny tasks** (median input ≪ 128 MB) → too many partitions; coalesce.
- **Few giant tasks** → too few partitions; repartition or raise shuffle
  partitions.

## 3. The triage playbook (say this in interviews)

1. **Is it one stage or all of them?** Open Jobs → Stages; find the stage eating
   the wall-clock.
2. **Skew or uniform-slow?** Compare task `max` vs `median`. Skew → §4 of
   [09 · Joins & AQE](09-joins-shuffle-aqe.md) (AQE skew join, salting, broadcast).
3. **Spilling?** Non-zero spill → repartition smaller, raise memory, or cut the
   data entering the shuffle.
4. **Right partition count?** Aim ~128–200 MB/partition; fix with
   `repartition`/`coalesce` or `spark.sql.shuffle.partitions` (+ AQE).
5. **Reading too much?** Check the scan: are `PushedFilters` present? Are you
   reading all columns? Partition-prune and column-prune (Parquet/Delta).
6. **Recompute thrash?** Reusing a DataFrame without `cache()` re-runs lineage —
   cache the reused, post-shuffle result.
7. **Confirm the fix in `explain()` and the UI**, don't assume.

## 4. Common errors and what they mean

| Symptom | Likely cause | Fix |
|---|---|---|
| **Driver `OutOfMemoryError`** | `collect()`/`toPandas()` pulled too much back | don't collect big data; `write` or `limit` |
| **Executor OOM** | partition too big / skew / huge hash build | more & smaller partitions, AQE skew, broadcast smaller side |
| **`Container killed by YARN, exceeding memory`** | overhead (off-heap/**Python**) exceeded | raise `spark.executor.memoryOverhead` |
| **`FetchFailedException`** | lost shuffle file / executor died / network | usually a retry; root cause is often OOM upstream or skew |
| **`GC overhead limit exceeded`** | heap churn | Kryo, less caching, more memory, fewer objects |
| **Job hangs at the last task** | classic **skew** straggler | de-skew (AQE/salting/broadcast) |
| **`AnalysisException`** | unresolved column/table, type mismatch | it's a *plan* error — fix the query, not the cluster |
| **`PythonException` / Py4J** | error inside a Python UDF | reproduce the UDF on a row locally; prefer built-ins/pandas-UDF |

## 5. Tools beyond the UI

- **`df.explain("formatted")`** — confirm pushdown, join strategy, exchanges
  ([09](09-joins-shuffle-aqe.md)).
- **`df.rdd.getNumPartitions()`** — sanity-check partitioning.
- **Accumulators / `spark.sql.metrics`** — count bad rows without a second pass.
- **Event logs + History Server** — post-mortem a finished/failed job.
- **`spark.sql.adaptive.localShuffleReader`, AQE markers** — verify adaptive
  kicked in.

## 6. Rapid-fire interview Q&A

- **First place you look for a slow job?** Spark UI → Stages → task time
  distribution.
- **How do you *see* skew?** One task's duration / shuffle-read far above the
  median.
- **What does spill mean?** A partition didn't fit in execution memory and was
  written to disk — a memory-pressure signal.
- **Driver OOM vs executor OOM?** Driver = collecting too much to one place;
  executor = a partition/hash too big for its heap.
- **"Container killed for exceeding memory limits" on a PySpark job?** Python runs
  in **overhead** memory — increase `memoryOverhead`.
- **`FetchFailedException` — what now?** Often an upstream OOM/skew killed an
  executor and took its shuffle files; fix the root memory/skew issue.
- **How do you confirm a tuning change worked?** Re-read `explain()` and the UI
  metrics — never assume.

---

**Next:** [02 · Performance Tuning & Debugging](02-performance-tuning.md) for the
catalog of levers, and [03 · Streaming Internals](03-streaming-internals.md) for
the real-time path.
