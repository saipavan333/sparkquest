# 02 · Performance Tuning & Debugging

> *Interview lens: juniors quote configs; seniors diagnose. The question is always
> "the job is slow / it OOMs — what do you do?" Answer with a **method**, not a
> grab-bag of flags. All defaults below are from the Spark SQL Performance Tuning
> guide — verify for your version.*

## 0. The diagnosis method (say this first)

1. **Open the Spark UI.** Find the slow **stage**. Look at the **task time
   distribution** (max vs median) and **shuffle read/write** and **spill**.
2. **Classify the problem:** skew (one task ≫ others), too few/many partitions,
   a giant shuffle, a bad join strategy, spill/GC, or small files.
3. **Fix the cause, re-measure.** Never tune blind.

Everything below maps to one of those symptoms.

## 1. Partitions: the master dial

Parallelism = `min(#partitions, total executor cores)`. Partitions too **big** →
spill, OOM, stragglers. Too **small** (many tiny tasks) → scheduling overhead.

- **Read side:** file sources split by `spark.sql.files.maxPartitionBytes`
  (default **128 MB**). 1 GB of Parquet ≈ 8 input partitions.
- **Shuffle side:** `spark.sql.shuffle.partitions` (default **200**) sets the
  post-shuffle partition count for joins/aggregations. The infamous default: a
  10 GB shuffle into 200 partitions = 50 MB each (fine); a 2 TB shuffle into 200 =
  10 GB each (spill hell). **Adaptive Query Execution now auto-tunes this** (§6),
  which is why you set a *large* initial value and let AQE coalesce.
- **`repartition(n)`** = full shuffle to exactly `n` (use to *increase* or to
  redistribute by key: `repartition("k")`). **`coalesce(n)`** = merge to `≤ n`
  with **no shuffle** (use to *reduce* partitions cheaply, e.g. before writing).
- **Target ~128 MB–200 MB per partition** as a rule of thumb.

## 2. Caching — and when *not* to

`cache()` (= `persist(MEMORY_AND_DISK)`) stores a DataFrame after its first
action so reuse is cheap. **Only cache what you reuse** — caching something used
once just wastes memory and can *evict* useful blocks. Storage levels trade memory
for CPU/disk: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `*_SER` (serialized, smaller but
CPU to deserialize), `DISK_ONLY`. Always `unpersist()` when done. Caching is also
a **lineage cut**: it stops re-computation from the source on every action.

## 3. Join strategies (know all four and the selection order)

| Strategy | How | Best when | Cost |
|---|---|---|---|
| **Broadcast Hash Join** | ship small side to every executor, hash-join locally | one side < ~10 MB (or `broadcast()`-hinted) | **no shuffle** — fastest |
| **Sort-Merge Join** | shuffle both sides by key, sort, merge | two large tables | 2 shuffles + sort |
| **Shuffle Hash Join** | shuffle both, build hash table on smaller side | one side fits in memory, no sort needed | shuffle, no sort |
| **Broadcast Nested Loop** | broadcast + nested loop | non-equi joins, tiny side | O(n·m) — avoid on big data |

Spark auto-broadcasts when a side's estimated size < `spark.sql.autoBroadcast
JoinThreshold` (**10 MB**). Force it with `F.broadcast(df)` or `/*+ BROADCAST(t)
*/`. **Hint priority:** `BROADCAST` > `MERGE` > `SHUFFLE_HASH` >
`SHUFFLE_REPLICATE_NL`. The biggest real-world win is broadcasting a dimension
table to avoid shuffling a billion-row fact table.

**Interview gotcha:** auto-broadcast relies on *statistics*. If stats are missing
(e.g. a freshly written path with no `ANALYZE TABLE`), Spark may overestimate and
pick a sort-merge join when a broadcast would've been ideal. Fix: run `ANALYZE
TABLE ... COMPUTE STATISTICS`, or hint the broadcast, or lean on **AQE** (§6),
which re-decides at runtime from *actual* shuffle sizes.

## 4. Data skew — the #1 senior interview topic

**Symptom:** in the Spark UI, one task runs for 40 minutes while the other 199
finish in 30 seconds. One key (`NULL`, `"unknown"`, a whale customer) holds most
rows, so one reduce task does all the work.

**Fixes, in order of preference:**

1. **Filter junk keys** — `NULL`/sentinel keys often dominate and aren't even
   needed. Cheapest fix.
2. **Broadcast the other side** if it's small — no shuffle, no skew.
3. **AQE skew join** (§6) — Spark *automatically* splits skewed partitions. On by
   default; often enough on its own.
4. **Salting** — when you must shuffle a skewed key: append a random salt
   `0..N-1` to the key on the big side, **explode** the small side across all `N`
   salts, join on `(key, salt)`, then aggregate away the salt. This spreads one
   hot key across `N` tasks. (The `perf-04` lesson does the core move.)
5. **Two-phase aggregation** — partial-aggregate with a salt, then final-
   aggregate without it.

## 5. Spill, OOM, and executor sizing

- **Spill** = execution memory ran out, so Spark wrote intermediate shuffle/sort
  data to disk. Visible as "Spill (memory)/(disk)" in the UI. Some spill is fine;
  heavy spill means partitions are too big → **increase partitions** or executor
  memory.
- **OOM** has flavors: **driver OOM** (you `collect()`ed too much, or broadcast a
  too-big table), **executor OOM** (partition too large / skew / a wide UDF
  buffering rows). Fix by reducing per-task data (more partitions, de-skew), not
  blindly raising memory.
- **Sizing rule of thumb:** prefer **"fat enough" executors** — e.g. 4–5 cores
  and ~’several GB per core’ — *not* one giant executor (GC pauses, lost
  parallelism) nor hundreds of 1-core executors (overhead, no shared broadcast).
  Leave ~1 core + ~1 GB per node for the OS/daemons. Each executor caches
  broadcasts **once** and shares them across its cores — an argument for
  multi-core executors.
- **Dynamic allocation** (`spark.dynamicAllocation.enabled`) scales executors up/
  down with load (needs an external shuffle service or shuffle tracking) — great
  for bursty/shared clusters.

## 6. Adaptive Query Execution (AQE) — default since Spark 3.2

AQE **re-optimizes the plan mid-flight** using real runtime statistics from
completed shuffle stages. Umbrella switch: `spark.sql.adaptive.enabled` (**true**).
It does three things you must be able to name:

1. **Coalesce post-shuffle partitions** — collapses the 200-default into the right
   number based on actual data, targeting `advisoryPartitionSizeInBytes`
   (**64 MB**). You no longer hand-tune `shuffle.partitions`; set it large and let
   AQE shrink it.
2. **Convert sort-merge → broadcast/shuffle-hash join** — if a side turns out
   small at runtime, AQE switches to a broadcast (or local-shuffle-reader) join,
   fixing the "stale stats" problem from §3.
3. **Skew join handling** (`spark.sql.adaptive.skewJoin.enabled`) — splits a
   partition deemed skewed (size > `skewedPartitionFactor` **×5** the median **and**
   > `skewedPartitionThresholdInBytes` **256 MB**) into several, automatically
   de-skewing sort-merge joins.

In an interview: *"AQE is why modern Spark mostly self-tunes shuffles and skew —
but it can't fix a fundamentally bad layout (small files, no partitioning) or a
`collect()` to the driver."*

## 7. Read & write layout

- **Small-files problem** — thousands of tiny output files murder read
  performance (per-file open cost, metadata) and stress the namenode/object store.
  Caused by high shuffle-partition counts on write. Fix with `coalesce`/
  `repartition` before write, the **`REBALANCE`** hint, or Delta **`OPTIMIZE`**
  (compaction).
- **Partitioning** (`partitionBy("date")`) — physical folders per value enabling
  **partition pruning** (read only `date=2026-01-01`). Partition on **low-
  cardinality** columns you filter on. Over-partitioning (high cardinality) *re-
  creates* the small-files problem.
- **Bucketing** (`bucketBy(n, "key")`) — hash pre-shuffle on a key at write time,
  so later joins/aggregations on that key skip the shuffle. Powerful for repeated
  joins on the same key; rigid (fixed bucket count).
- **Columnar formats (Parquet/ORC)** — column pruning + predicate pushdown via
  row-group min/max stats. Always prefer them over CSV/JSON for analytics.

## 8. The Spark UI, like a detective

- **Jobs/Stages tab** — find the long stage; open its **task table**, sort by
  duration. Max ≫ median ⇒ **skew**. Many tiny tasks ⇒ over-partitioned.
- **Shuffle Read/Write** columns — big numbers ⇒ a costly exchange; can you
  broadcast or pre-bucket it away?
- **Spill (memory/disk)** — partitions too big.
- **SQL tab** — the physical plan with per-operator row counts and `isRuntime`
  stats (AQE). Spot `Exchange` (shuffles), join type, `PushedFilters`.
- **Executors tab** — GC time (high ⇒ memory pressure), failed tasks, data
  locality.

> Master sentence for any "it's slow" question: *"I'd open the Spark UI, find the
> bottleneck stage, check the task-time distribution for skew and the shuffle/spill
> metrics, then fix the specific cause — broadcast, de-skew, repartition, or
> better layout — and re-measure."*

---

**Next:** [03 · Structured Streaming Internals](03-streaming-internals.md).
