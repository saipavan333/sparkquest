# 09 · Joins, Shuffle & Adaptive Query Execution (Deep-Dive)

> *Interview lens: joins are where Spark interviews are won or lost. If you can
> name all five join strategies, explain exactly when Spark picks each, read a
> physical plan to confirm it, and de-skew a slow join three different ways, you
> are in the top decile of candidates.*
>
> Prereqs: [01 · Architecture & Execution](01-spark-architecture-and-execution.md)
> (shuffle, stages) and [02 · Performance Tuning](02-performance-tuning.md).
> Source of truth: the official
> [SQL Performance Tuning guide](https://spark.apache.org/docs/3.5.1/sql-performance-tuning.html).

## 1. The shuffle, precisely

A **shuffle** is a redistribution of rows across partitions by a key, and it is
the foundation of every wide transformation (`join`, `groupBy`, `distinct`,
`orderBy`, `repartition`, window functions). It happens in two halves separated
by a stage boundary:

- **Map side (write).** Each task in the upstream stage partitions its output by
  `hash(key) % numPartitions`, writes one **shuffle block** per target partition
  to *local disk*, and (for aggregations/joins) may pre-aggregate or sort. These
  files are served by the executor's **external shuffle service**.
- **Reduce side (read/fetch).** Each task in the downstream stage **pulls** its
  blocks from every upstream executor over the network, then merges/sorts/joins.

The number of reduce-side partitions is `spark.sql.shuffle.partitions`
(**default 200**) unless AQE coalesces them (§5). The shuffle is expensive in
three currencies — **disk I/O** (spill + shuffle files), **network** (all-to-all
fetch), and **CPU/serialization** — and it is where **skew** and **spill** hurt.
Tuning Spark is mostly *minimizing, sizing, and de-skewing shuffles.*

## 2. The five join strategies

Spark physically executes a join with one of five operators. Know all five.

### a. Broadcast Hash Join (BHJ) — the one you want

The small side is **collected to the driver, broadcast to every executor**, and
built into an in-memory hash table; the large side is streamed and probed
locally. **No shuffle of the large table.** This is the fastest join for
*small ⋈ large* and the single most important optimization to reach for.

- Trigger: one side's estimated size ≤ `spark.sql.autoBroadcastJoinThreshold`
  (**default 10 MB**, `10485760`; set `-1` to disable), **or** a `BROADCAST` hint.
- Requires an **equi-join** (at least one `=` predicate). Supports inner and the
  outer side opposite the broadcast (you can't broadcast the streamed side of a
  left outer's left table).
- Cost: `O(n)` probe, but the small side must fit in driver and executor memory.
  Broadcasting something too big → driver OOM or slow broadcast.

### b. Shuffle Hash Join (SHJ)

Both sides are **shuffled by the join key**; then, per partition, Spark builds a
hash table on the **smaller** side and probes with the larger. **No sort.**

- Chosen when a side is small enough to build a per-partition hash table but too
  big to broadcast, **and** `spark.sql.join.preferSortMergeJoin=false` (default
  is `true`, so SHJ is off the table unless hinted) — or via a `SHUFFLE_HASH` hint.
- Faster than SMJ when it applies (no sort), but the hash table can OOM if the
  build side per partition is large; SMJ is safer for very large inputs.

### c. Sort-Merge Join (SMJ) — the workhorse for large ⋈ large

Both sides are shuffled by key, **each partition is sorted by key**, then the two
sorted streams are merged. This is the **default** for large equi-joins.

- Handles arbitrarily large inputs (sort can spill to disk); needs **sortable**
  (orderable) join keys.
- Cost is dominated by the shuffle + sort. The `Exchange` + `Sort` operators
  above a `SortMergeJoin` in the plan are the tell.

### d. Broadcast Nested Loop Join (BNLJ)

Broadcast one side, then **nested-loop** every row of the other against it. Used
for **non-equi joins** (`<`, `BETWEEN`, range) or joins with no key, when one
side is small. `O(n·m)` but tolerable when `m` (broadcast side) is tiny.

### e. Cartesian / Shuffle-and-Replicate Nested Loop

The full **cross product**, used for `crossJoin` or a non-equi join where neither
side can be broadcast. Genuinely `O(n·m)` with a shuffle-replicate — the
slowest path; usually a bug if you didn't intend a cross join.

| Strategy | Shuffle? | Sort? | Join type | Picked when |
|---|:--:|:--:|---|---|
| **Broadcast Hash** | no (broadcast) | no | equi | a side ≤ broadcast threshold / hint |
| **Shuffle Hash** | yes | no | equi | a side small-ish, `preferSortMergeJoin=false`/hint |
| **Sort-Merge** | yes | yes | equi | default for large ⋈ large |
| **Broadcast Nested Loop** | no (broadcast) | no | non-equi | one side tiny |
| **Cartesian** | replicate | no | cross / non-equi | last resort |

## 3. How Spark chooses (the decision order)

For an **equi-join**, Spark (per the SparkStrategies source and the tuning docs)
resolves in roughly this priority:

1. **Hints win**, in order `BROADCAST` > `SHUFFLE_HASH` > `SHUFFLE_MERGE`
   (sort-merge) > `SHUFFLE_REPLICATE_NL`. If both sides hint `BROADCAST`, the
   smaller is broadcast.
2. Else if one side is ≤ `autoBroadcastJoinThreshold` and the join type allows it
   → **Broadcast Hash Join**.
3. Else if `preferSortMergeJoin=false` and one side is small enough to build a
   hash table → **Shuffle Hash Join**.
4. Else if keys are sortable → **Sort-Merge Join**.

For a **non-equi join**: Broadcast Nested Loop if a side is broadcastable, else
Cartesian. With **AQE** on (§5), step 2 is re-evaluated *at runtime* using actual
shuffle statistics — so a join planned as SMJ can be demoted to BHJ once Spark
learns a side is small.

### Join hints (force a strategy)

```python
from pyspark.sql.functions import broadcast
big.join(broadcast(small), "id")              # DataFrame API
# SQL: SELECT /*+ BROADCAST(small) */ ...
#      /*+ MERGE(a,b) */, /*+ SHUFFLE_HASH(t) */, /*+ SHUFFLE_REPLICATE_NL(t) */
```

`broadcast()` is the most useful — it overrides the size estimate when you *know*
a table is small (the optimizer's estimate can be wrong after filters/joins).

## 4. Reading a join in the physical plan

`df.explain()` (or `explain("formatted")`) is how you *prove* what Spark did:

- `Exchange hashpartitioning(key, 200)` → a **shuffle** on `key`.
- `BroadcastExchange` + `BroadcastHashJoin` → a **broadcast join** (no big shuffle).
- `SortMergeJoin` sitting above two `Sort` + `Exchange` → a **sort-merge join**.
- `*(n)` prefix → **whole-stage codegen** fused that operator group.
- `PushedFilters: [...]` on a scan → **predicate pushdown** reached the source.
- With AQE, look for `AdaptiveSparkPlan isFinalPlan=true` and
  `AQEShuffleRead coalesced`/`skewed` markers in the final plan.

> **Interview move:** "I'd run `explain('formatted')`, confirm a `BroadcastHashJoin`
> with no `Exchange` on the big side; if I saw a `SortMergeJoin` with a giant
> `Exchange`, I'd check the size estimate and add a `broadcast()` hint."

## 5. Adaptive Query Execution (AQE) — runtime re-planning

AQE re-optimizes the plan **using real statistics gathered after each shuffle**,
rather than trusting compile-time estimates. It is **enabled by default since
Spark 3.2** (`spark.sql.adaptive.enabled=true`). Three features:

### a. Dynamically coalescing shuffle partitions

After a shuffle, AQE **merges** small adjacent partitions up to a target size, so
you don't get 200 tiny tasks when the data is small. Controlled by:

- `spark.sql.adaptive.coalescePartitions.enabled` (default `true`)
- `spark.sql.adaptive.advisoryPartitionSizeInBytes` (default **64 MB**) — the
  target post-coalesce partition size.

This is why you can leave `spark.sql.shuffle.partitions` high (e.g. 200/2000) and
let AQE right-size it instead of hand-tuning per query.

### b. Converting sort-merge → broadcast at runtime

If, *after* a shuffle, AQE sees a join side is actually smaller than
`spark.sql.adaptive.autoBroadcastJoinThreshold`, it rewrites the SMJ into a
**broadcast hash join** — catching the small tables that compile-time estimates
missed (e.g. a table that became tiny after a selective filter).

### c. Skew join optimization

AQE detects **skewed partitions** in a sort-merge join and **splits** them into
smaller sub-partitions, replicating the matching side, so one giant task doesn't
hold up the stage. Requires `spark.sql.adaptive.enabled` **and**
`spark.sql.adaptive.skewJoin.enabled` (default `true`). A partition is "skewed"
when it is both:

- larger than `skewedPartitionThresholdInBytes` (default **256 MB**), **and**
- larger than `skewedPartitionFactor` × the *median* partition size (default
  factor **5.0**).

## 6. Data skew — the classic join killer

**Skew** = a few keys hold most of the rows (a "celebrity" user, a `NULL` foreign
key, one mega-merchant). One reduce task gets a huge partition; the stage's
runtime is hostage to that straggler while other cores sit idle. Symptoms in the
**Spark UI**: one task with max duration / shuffle-read 100× the median, heavy
**spill**.

Three fixes, strongest interview answer names all three:

1. **Let AQE handle it** (§5c) — often enough; free.
2. **Salting** — break the hot key into `N` synthetic sub-keys so its rows spread
   across `N` partitions, join against a replicated small side, then aggregate:

   ```python
   from pyspark.sql import functions as F
   N = 16
   big_salted = big.withColumn("salt", (F.rand() * N).cast("int"))
   small_exploded = (small
       .withColumn("salt", F.explode(F.array([F.lit(i) for i in range(N)]))))
   joined = big_salted.join(small_exploded, ["id", "salt"]).drop("salt")
   ```

3. **Broadcast the small side** — if the other table is small, a broadcast join
   sidesteps the shuffle entirely, so skew on the join key stops mattering.
4. **Filter/handle `NULL` keys** separately — `NULL`s never match but all hash to
   one partition; split them out or filter before the join.

## 7. Minimizing shuffle (the levers)

- **Broadcast** small dimensions instead of shuffling them.
- **Pre-partition / bucket** tables by the join key so repeated joins skip the
  shuffle (`bucketBy` on write; see [04 · File Formats & Layout](04-file-formats-and-layout.md)).
- **Filter and project early** (Catalyst does this, but writing it explicitly
  keeps plans readable) so less data enters the shuffle.
- **`reduceByKey`/`groupBy().agg()` with combinable aggregates** ship partial
  results, not raw rows (map-side combine).
- Reuse a shuffled/sorted layout (`repartition` once, join several times).

## 8. Rapid-fire interview Q&A

- **Default join for two large tables?** Sort-Merge Join.
- **How do you speed up a small-vs-large join?** Broadcast the small side
  (`broadcast()` or rely on the 10 MB auto threshold).
- **Why might a broadcast join fail?** Small side isn't actually small → driver
  OOM / slow broadcast; or it's a non-equi join (then BNLJ).
- **What is `spark.sql.shuffle.partitions` and its default?** Reduce-side
  partition count for shuffles; default **200**. AQE can coalesce it down.
- **Your join has one task running 10× longer — diagnosis?** Skew. Fix with AQE
  skew join, salting, or broadcast.
- **What does AQE do?** Re-plans at runtime with real stats: coalesce partitions,
  SMJ→broadcast conversion, skew-join splitting.
- **SHJ vs SMJ?** SHJ builds a hash table (no sort, faster) but can OOM on big
  build sides; SMJ sorts and merges, safe for very large inputs. SMJ is preferred
  by default (`preferSortMergeJoin=true`).

---

**Next:** [10 · RDDs & the Low-Level API](10-rdd-and-low-level-api.md) — the layer
beneath DataFrames, and the few times you should drop to it.
