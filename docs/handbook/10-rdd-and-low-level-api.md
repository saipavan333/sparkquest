# 10 · RDDs & the Low-Level API

> *Interview lens: you'll write DataFrames 99% of the time, but interviewers probe
> RDDs to check you understand what's *underneath* — partitions, lineage,
> `reduceByKey` vs `groupByKey`, broadcast variables, and accumulators. Knowing
> when **not** to drop to RDDs is itself a senior signal.*
>
> Source of truth: the official
> [RDD Programming Guide](https://spark.apache.org/docs/3.5.1/rdd-programming-guide.html).

## 1. What an RDD actually is

An **RDD** (Resilient Distributed Dataset) is the original Spark abstraction: an
**immutable, partitioned** collection of objects that can be operated on in
parallel. Five properties define it (the ones the scheduler reads):

1. A list of **partitions** (the unit of parallelism).
2. A **compute** function to produce each partition.
3. A list of **dependencies** on parent RDDs (the **lineage**).
4. Optionally, a **Partitioner** (e.g. hash) for key-value RDDs.
5. Optionally, **preferred locations** for each partition (data locality).

"**Resilient**" = if a partition is lost, Spark **recomputes** it from lineage
rather than replicating data. "**Distributed**" = partitions live across
executors. RDDs are **lazy** (transformations build lineage; actions execute) —
the same model DataFrames inherit.

## 2. Creating RDDs and controlling partitions

```python
sc = spark.sparkContext
rdd = sc.parallelize([1, 2, 3, 4], numSlices=4)   # from a local collection
lines = sc.textFile("data.txt", minPartitions=8)   # from storage (one+ partition/block)
print(rdd.getNumPartitions())
```

Partition count = the parallelism ceiling. Too few starves cores; too many adds
scheduler overhead. `repartition(n)` (shuffle) or `coalesce(n)` (no shuffle, only
shrinks) adjust it.

## 3. Transformations vs actions

**Transformations** are lazy and return a new RDD:

- Element-wise (narrow): `map`, `flatMap`, `filter`, `mapPartitions`,
  `mapPartitionsWithIndex`, `distinct`, `sample`, `union`.
- Key-value / wide (shuffle): `reduceByKey`, `groupByKey`, `aggregateByKey`,
  `combineByKey`, `sortByKey`, `join`, `cogroup`, `partitionBy`.

**Actions** force execution and return a value to the driver or write output:
`collect`, `count`, `first`, `take(n)`, `reduce`, `fold`, `aggregate`,
`foreach`, `saveAsTextFile`, `countByKey`, `takeOrdered`.

### `reduceByKey` vs `groupByKey` — the canonical RDD interview question

Both group by key, but:

- **`reduceByKey(f)`** combines values **map-side first** (a combiner), so each
  partition ships one partial result per key across the shuffle. Far less network.
- **`groupByKey()`** shuffles **every value** to one place, then you reduce. It
  can **OOM** on a hot key because all values for that key must fit in memory.

> **Always prefer `reduceByKey`/`aggregateByKey` over `groupByKey` + reduce.**
> Same logic for DataFrames: `groupBy().agg(sum)` does a map-side combine;
> `collect_list` does not.

```python
# Word count — the canonical example
counts = (sc.textFile("data.txt")
            .flatMap(lambda line: line.split())
            .map(lambda w: (w, 1))
            .reduceByKey(lambda a, b: a + b))   # map-side combine
```

## 4. Persistence & storage levels

By default each action **re-executes the full lineage**. Persist an RDD you reuse:

```python
rdd.cache()                       # = persist(MEMORY_ONLY)
rdd.persist(StorageLevel.MEMORY_AND_DISK)
rdd.unpersist()                   # release it
```

Storage levels trade memory for CPU/resilience:

| Level | Meaning |
|---|---|
| `MEMORY_ONLY` | deserialized objects in RAM; partitions that don't fit are **recomputed** (RDD default) |
| `MEMORY_AND_DISK` | spill non-fitting partitions to disk (DataFrame `.cache()` default) |
| `DISK_ONLY` | always on disk |
| `*_2` (e.g. `MEMORY_ONLY_2`) | replicate each partition on **two** nodes for fault tolerance |
| `OFF_HEAP` | serialized in off-heap memory (Tungsten region) |

> **PySpark note:** in Python, cached objects are **always serialized with
> Pickle**, so the `_SER` levels are equivalent to their plain counterparts. The
> Python-available levels are `MEMORY_ONLY`, `MEMORY_ONLY_2`, `MEMORY_AND_DISK`,
> `MEMORY_AND_DISK_2`, `DISK_ONLY`, `DISK_ONLY_2`, `DISK_ONLY_3`.

Rule of thumb (from the docs): if it fits, keep `MEMORY_ONLY`; if not,
`MEMORY_AND_DISK` to avoid recompute; use `_2` only when recompute is more
expensive than the extra storage.

## 5. Shared variables: broadcast & accumulators

Closures sent to tasks normally ship a **copy** of any referenced variable to
every task. Two special variables fix the two problems that causes.

### Broadcast variables — ship a big read-only value once per executor

```python
table = {"US": "United States", "IN": "India"}
b = sc.broadcast(table)                       # cached once per executor
rdd.map(lambda code: b.value[code])           # read with .value
```

Use for a large lookup map/model you reference in many tasks — broadcasting sends
it **once per executor** (not once per task), using an efficient BitTorrent-like
protocol. This is the RDD-level cousin of `F.broadcast()` for DataFrame joins
([09 · Joins](09-joins-shuffle-aqe.md)).

### Accumulators — add-only aggregates back to the driver

```python
errors = sc.accumulator(0)
def parse(line):
    global errors
    if bad(line):
        errors += 1
    return clean(line)
rdd.map(parse).count()
print(errors.value)        # only the DRIVER should read .value
```

Accumulators are "add-only" counters/sums updated by executors and read by the
driver. **Caveat interviewers love:** Spark guarantees an accumulator is applied
**exactly once only for actions**; inside *transformations*, a task retry or
recomputation can apply the update **more than once**, so don't rely on
accumulator values for correctness in transformations — use them for metrics/debug.

## 6. When to use RDDs (and when not to)

**Prefer DataFrames/SQL** for virtually everything: Catalyst optimizes them,
Tungsten gives off-heap binary storage and whole-stage codegen, and in PySpark
DataFrame ops run in the JVM with no per-row Python round-trip.

Drop to RDDs only for:

- Logic the DataFrame API can't express (complex custom partitioning, certain
  iterative/graph algorithms, non-tabular data).
- Fine-grained control over physical execution (`mapPartitions` to amortize a
  per-partition setup like a DB connection).

**The PySpark tax:** an RDD `map(lambda ...)` serializes every row across the
JVM↔Python boundary and runs your lambda in a Python worker — slow, and **opaque
to Catalyst** (it can't see inside your lambda to optimize). A DataFrame
expression stays in the JVM. This is why `df.filter("x > 0")` beats
`rdd.filter(lambda r: r.x > 0)`, and why **pandas UDFs** (Arrow-batched) beat
plain Python UDFs when you must run Python.

## 7. RDD ⇄ DataFrame interop

```python
df = rdd.map(lambda x: (x, x*x)).toDF(["n", "square"])   # RDD → DataFrame
rdd2 = df.rdd                                            # DataFrame → RDD[Row]
```

Useful at the edges, but each crossing has a cost; stay in DataFrames when you can.

## 8. Rapid-fire interview Q&A

- **What makes an RDD "resilient"?** Lineage — lost partitions are recomputed, not
  replicated.
- **`reduceByKey` vs `groupByKey`?** `reduceByKey` combines map-side (less
  shuffle, no OOM on hot keys); avoid `groupByKey`.
- **`cache()` vs `persist()`?** `cache()` = `persist()` with the default level
  (`MEMORY_ONLY` for RDD, `MEMORY_AND_DISK` for DataFrame).
- **Broadcast variable vs broadcast join?** Same idea — ship a small read-only
  thing once per executor; the join is the SQL-layer application.
- **Are accumulators reliable?** Exactly-once only in **actions**; transformations
  may double-count on retries — metrics only.
- **Why are DataFrames faster than RDDs in PySpark?** Catalyst + Tungsten + no
  per-row Python serialization; RDD lambdas are opaque and pay the JVM↔Python tax.
- **`repartition` vs `coalesce`?** `repartition(n)` reshuffles (can grow or
  shrink, balanced); `coalesce(n)` only shrinks without a full shuffle.

---

**Next:** [02 · Performance Tuning & Debugging](02-performance-tuning.md) applies
all of this to real slow-job triage.
