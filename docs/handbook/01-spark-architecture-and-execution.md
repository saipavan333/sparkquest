# 01 · Spark Architecture & Execution

> *Interview lens: if you can draw the path from `df.groupBy(...).count().show()`
> to tasks running on executors — and name what Catalyst and Tungsten did along
> the way — you've cleared the bar most candidates fail.*

## 1. The cluster: driver and executors

A Spark application is one **driver** process coordinating many **executor**
processes.

- **Driver** — runs your `main`/notebook, holds the `SparkSession`, builds the
  logical/physical plans, maintains the **DAG scheduler** and **task scheduler**,
  and tracks metadata for every partition. It does *not* process your data; it
  orchestrates. If the driver dies, the app dies. `collect()`-ing a huge result
  OOMs the **driver** (a classic bug) because all data is pulled back to it.
- **Executors** — JVM processes on worker nodes that actually run **tasks** and
  hold cached data in memory. Each executor has a fixed number of **cores**
  (task slots) and a memory budget. Executors are launched by the **cluster
  manager** (YARN, Kubernetes, Standalone, or `local[k]` on one JVM).
- **Cluster manager** — negotiates resources. Spark is agnostic to which one.

A useful mental model: *driver = brain, executors = hands, cluster manager =
HR that hires the hands.*

## 2. Lazy evaluation: transformations vs actions

Spark operations are **transformations** (lazy — build a recipe) or **actions**
(eager — execute the recipe).

- **Transformations** (`select`, `filter`, `withColumn`, `join`, `groupBy`) return
  a new DataFrame and run *nothing*. They just extend the lineage.
- **Actions** (`count`, `collect`, `show`, `write`, `take`, `foreach`) trigger a
  **job**: Spark finally plans and executes everything needed to produce that
  result.

Why lazy? It lets Catalyst see the *whole* query and optimize globally — push
filters down to the scan, prune columns, reorder joins, collapse projections.
An eager system can't do that.

**Interview trap:** "Does `df.filter(...)` run on the cluster?" No — nothing runs
until an action. **And** every action re-executes the lineage from the last
cached/shuffled point, which is *why you `cache()`* a DataFrame you reuse.

## 3. Narrow vs wide transformations (the shuffle boundary)

- **Narrow** — each output partition depends on **one** input partition. No data
  moves between nodes. Examples: `map`, `filter`, `select`, `withColumn`,
  `union`. These pipeline together within a stage.
- **Wide** — each output partition depends on **many** input partitions, so Spark
  must **shuffle**: repartition data across the network by key. Examples:
  `groupBy`, `join`, `distinct`, `repartition`, `orderBy`, window functions.

The shuffle is the single most important performance concept in Spark. It writes
intermediate files to local disk on the *map side*, then the *reduce side* fetches
them over the network. It's expensive in **disk I/O, network, and serialization**,
and it's where **skew** and **spill** bite. Minimizing and de-skewing shuffles is
80% of Spark tuning (see [chapter 02](02-performance-tuning.md)).

## 4. Jobs → stages → tasks (the unit hierarchy)

When an action fires:

1. The **DAG scheduler** splits the job into **stages** at every shuffle
   boundary. All narrow transformations between two shuffles collapse into one
   stage (pipelined).
2. Each stage becomes a set of **tasks** — **one task per partition**. A task is
   the smallest unit of work; it runs your stage's code on a single partition on
   a single core.
3. The **task scheduler** ships tasks to executor cores, respecting **data
   locality** (run the task where its data already is, if possible).
4. Stages run in dependency order; tasks *within* a stage run in parallel, bounded
   by `total executor cores`.

So: **1 action → 1+ jobs → N stages (split by shuffles) → M tasks (= partitions).**
Parallelism is capped by `min(number of partitions, total cores)`. Too few
partitions starves cores; too many adds scheduling overhead. This is exactly why
`spark.sql.shuffle.partitions` (default **200**) and partition sizing matter.

## 5. RDD vs DataFrame vs Dataset

| | RDD | DataFrame | Dataset |
|---|---|---|---|
| Abstraction | distributed objects | distributed table (named columns) | typed table |
| Optimizer | none (you optimize) | **Catalyst** | Catalyst |
| Serialization | Java/Kryo | **Tungsten** (binary, off-heap) | Tungsten + encoders |
| Type safety | compile-time | runtime (untyped `Row`) | compile-time (Scala/Java) |
| Use it when | low-level control, non-tabular | **everything, in PySpark** | typed JVM pipelines |

**Key point for PySpark:** there is no typed `Dataset` API in Python — you use
**DataFrames**, and you should, because Catalyst + Tungsten make them far faster
than RDDs. Drop to RDDs only for things the DataFrame API can't express. RDDs are
opaque to Catalyst (it can't optimize your lambdas), and Python RDD lambdas pay a
serialization tax crossing the JVM↔Python boundary.

## 6. Catalyst: the query optimizer

Catalyst turns your DataFrame/SQL into an optimized physical plan in **four
phases**:

1. **Analysis** — resolve column/table names against the catalog, check types.
   Produces a *resolved logical plan*. (Unresolved references → `AnalysisException`.)
2. **Logical optimization** — apply rule-based rewrites: **predicate pushdown**
   (move filters toward the scan), **column/projection pruning** (read only
   needed columns), constant folding, boolean simplification, filter/join
   reordering, collapsing projections.
3. **Physical planning** — generate one or more physical plans (e.g. which **join
   strategy**), and pick one using a **cost model**.
4. **Code generation** — Tungsten's **whole-stage code generation** fuses an
   entire stage of operators into a single compiled Java function (see §7).

Read the result with `df.explain(True)` (parsed → analyzed → optimized →
physical) or `df.explain("formatted")`. In interviews, being able to *read a
physical plan* — spotting `Exchange` (a shuffle), `BroadcastHashJoin` vs
`SortMergeJoin`, `*(1)` whole-stage-codegen markers, and `PushedFilters` — is gold.

**Predicate pushdown** and **column pruning** are the two optimizations to name
on demand: with Parquet, Spark reads only the needed columns (columnar) and skips
row groups whose min/max stats can't match the filter — often a 10–100× I/O win.

## 7. Tungsten: memory & code generation

Catalyst decides *what* to do; **Tungsten** makes the execution fast:

- **Binary off-heap memory** — store rows in a compact binary format
  (`UnsafeRow`), bypassing the JVM object model and slashing GC pressure. Spark
  manages this memory itself rather than leaning on the garbage collector.
- **Cache-aware computation** — algorithms and data layouts tuned for CPU cache
  lines (e.g. cache-aware sorting).
- **Whole-stage code generation** — instead of an interpreter calling a chain of
  iterator `next()`s (one virtual call per row per operator), Tungsten compiles
  the whole stage into one tight loop of Java bytecode. In the physical plan,
  operators marked with `*` and a `(n)` codegen id are fused. This is why modern
  DataFrame Spark can be an order of magnitude faster than RDD-era Spark.

## 8. Memory model (one slide, because interviewers ask)

Within an executor, **unified memory** splits a region between:

- **Execution memory** — shuffles, joins, sorts, aggregations (short-lived).
- **Storage memory** — cached/persisted blocks (long-lived).

They share one pool and **borrow** from each other: execution can evict cached
blocks under pressure (execution wins, because a stalled shuffle blocks progress).
Beyond that sits **user memory** (your data structures, UDF state) and a small
**reserved** region. When execution can't get enough memory, it **spills** to
disk; when it truly can't proceed (or the driver collects too much), you get an
**OOM**. Sizing this is [chapter 02](02-performance-tuning.md).

## 9. End-to-end: what happens on `df.groupBy("k").count().show()`

1. `groupBy().count()` are **transformations** — lineage only.
2. `show()` is an **action** → triggers a **job**.
3. Catalyst analyzes, pushes filters/prunes columns, plans a **hash aggregate**
   with a shuffle on `k`.
4. The DAG scheduler splits at the shuffle into **stage 1** (scan + partial/“map-
   side” aggregate) and **stage 2** (fetch shuffle + final aggregate).
5. Stage 1 runs **one task per input partition**; each does a *partial* count
   locally (combiner), then writes shuffle files bucketed by `hash(k) %
   numShufflePartitions`.
6. Stage 2 runs `spark.sql.shuffle.partitions` tasks; each fetches its buckets,
   merges partial counts, emits final rows.
7. `show()` pulls the first 20 rows back to the **driver**.

Notice the **map-side combine**: Spark counts locally *before* shuffling, so it
ships partial aggregates, not raw rows — a huge network saving. That's why
`groupBy().count()` is cheap but `groupBy().collect_list()` (no combine) can blow
up.

---

**Next:** [02 · Performance Tuning & Debugging](02-performance-tuning.md) turns
this model into the levers you pull when a job is slow.
