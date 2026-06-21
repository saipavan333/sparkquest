# 11 · Configuration & Cluster Sizing

> *Interview lens: "You have a 100-node cluster, 16 cores and 64 GB per node — how
> many executors, how many cores each, how much memory?" If you can do this math
> out loud and justify every number, you read as someone who has actually run
> Spark in production.*
>
> Source of truth: the official
> [Configuration](https://spark.apache.org/docs/3.5.1/configuration.html) and
> [Tuning](https://spark.apache.org/docs/3.5.1/tuning.html) guides.

## 1. Where Spark runs: deploy modes & cluster managers

- **Cluster managers** (Spark is agnostic): **Standalone**, **YARN**,
  **Kubernetes**, **Mesos** (deprecated), and `local[k]` (one JVM, `k` threads —
  what SparkQuest uses).
- **Deploy mode** — where the **driver** runs:
  - `--deploy-mode client`: driver runs on the submitting machine (your laptop /
    edge node). Good for interactive work; the driver must stay connected.
  - `--deploy-mode cluster`: driver runs **inside the cluster** as a managed
    process. Good for production / fire-and-forget jobs.

```bash
spark-submit \
  --master yarn --deploy-mode cluster \
  --num-executors 50 --executor-cores 5 --executor-memory 19g \
  --driver-memory 8g --conf spark.sql.shuffle.partitions=400 \
  app.py
```

## 2. The resource trinity: executors × cores × memory

Three knobs set your parallelism and memory:

- **`spark.executor.instances`** (`--num-executors`) — how many executor JVMs.
- **`spark.executor.cores`** (`--executor-cores`) — task slots per executor
  (concurrency = total cores = executors × cores).
- **`spark.executor.memory`** (`--executor-memory`) — heap per executor.

**Total concurrent tasks = num-executors × executor-cores.** That is your real
parallelism ceiling.

### Fat vs thin executors (the trade-off interviewers want)

- **Thin** (1 core each): no in-executor parallelism, poor broadcast/shared-memory
  reuse, tons of JVMs → overhead.
- **Fat** (all cores in one executor): HDFS/object-store I/O throughput collapses
  past ~5 concurrent threads, and GC on a huge heap stalls.
- **Sweet spot: ~5 cores per executor** — the long-standing rule of thumb that
  balances I/O throughput against JVM/GC overhead.

### Worked sizing example

Node: 16 cores, 64 GB. Reserve 1 core + ~1 GB for the OS/daemons → 15 cores,
63 GB usable.

1. Cores per executor = **5** → `floor(15 / 5)` = **3 executors per node**.
2. Memory per executor = `63 GB / 3` ≈ 21 GB. Subtract ~**10% overhead**
   (`spark.executor.memoryOverhead`, min 384 MB) → set `--executor-memory ~19g`.
3. Across 10 nodes: `10 × 3` = 30 executors; leave **1 executor for the
   application master** → `--num-executors 29`.

That's `29 × 5 = 145` concurrent tasks — so size
`spark.sql.shuffle.partitions` to a small multiple of that (e.g. 290–580), not
the default 200.

## 3. The executor memory model (sizing the regions)

Inside an executor's heap (after a ~300 MB **reserved** region):

- **Unified memory** = `spark.memory.fraction` × (heap − 300 MB), default
  **0.6**. Shared by:
  - **Execution** — shuffles, joins, sorts, aggregations (short-lived).
  - **Storage** — cached/persisted blocks (long-lived).
  They **borrow** from each other; `spark.memory.storageFraction` (default
  **0.5**) is the storage portion that's *immune* from eviction.
- **User memory** = the remaining ~40% — your UDF state, data structures.
- **Off-heap** — `spark.memory.offHeap.enabled` + `spark.memory.offHeap.size`
  for Tungsten binary storage outside the GC's reach.
- **Overhead** — `spark.executor.memoryOverhead` (~10%, min 384 MB) covers
  off-heap, Python workers, netty buffers. **PySpark/pandas-UDF jobs need more
  overhead** because Python lives here — a frequent "container killed by YARN for
  exceeding memory limits" cause.

## 4. Parallelism & partition sizing

- **`spark.sql.shuffle.partitions`** (default **200**) — reduce-side partitions
  for SQL/DataFrame shuffles. Target **~2–3× total cores**, or let **AQE**
  coalesce (see [09 · Joins & AQE](09-joins-shuffle-aqe.md)).
- **`spark.default.parallelism`** — default partitions for **RDD** ops; set to
  total cores.
- **Target partition size ≈ 128–200 MB.** Too big → spill/OOM and skew; too small
  → scheduler overhead dominates. Diagnose with task input size in the Spark UI.

## 5. Dynamic allocation (elastic executors)

Let Spark add/remove executors based on pending tasks:

```
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=2
spark.dynamicAllocation.maxExecutors=100
spark.dynamicAllocation.shuffleTracking.enabled=true   # or an external shuffle service
```

Great for shared/bursty clusters and idle-cost control; it needs **shuffle
tracking** or an **external shuffle service** so shuffle files survive executor
removal. The trade-off is ramp-up latency and noisier-neighbor scheduling.

## 6. Serialization & misc levers

- **`spark.serializer`** — default `JavaSerializer`; switch to
  `org.apache.spark.serializer.KryoSerializer` for smaller, faster shuffle/cache
  (register your classes for max benefit). Big win on RDD-heavy jobs.
- **`spark.sql.files.maxPartitionBytes`** (default 128 MB) — read-side partition
  sizing for file scans.
- **`spark.speculation`** — relaunch straggler tasks on another node (helps with
  flaky hardware, not data skew).
- **`spark.sql.autoBroadcastJoinThreshold`** (10 MB) — broadcast-join cutoff.

| Config | Default | What it controls |
|---|---|---|
| `spark.executor.cores` | (mgr-dependent) | task slots per executor (~5 sweet spot) |
| `spark.executor.memory` | 1g | executor heap |
| `spark.executor.memoryOverhead` | ~10% (min 384m) | off-heap/Python/netty |
| `spark.memory.fraction` | 0.6 | unified exec+storage pool |
| `spark.memory.storageFraction` | 0.5 | eviction-immune storage portion |
| `spark.sql.shuffle.partitions` | 200 | shuffle reduce partitions |
| `spark.default.parallelism` | total cores | RDD default partitions |
| `spark.sql.files.maxPartitionBytes` | 128MB | scan partition size |
| `spark.serializer` | Java | shuffle/cache serialization |
| `spark.dynamicAllocation.enabled` | false | elastic executors |

## 7. Setting config (precedence)

`SparkConf` / `spark-submit --conf` / `spark.conf.set(...)` at runtime, in
increasing specificity. **SQL/runtime** configs (`spark.sql.*`) can be set after
the session starts; **cluster** configs (executor cores/memory, instances) must
be set **at submit time** — you can't resize executors mid-application.

```python
spark.conf.set("spark.sql.shuffle.partitions", 400)   # runtime OK
# spark.executor.memory must be set at submit / SparkSession build time
```

## 8. Rapid-fire interview Q&A

- **How many cores per executor?** ~5 — balances I/O throughput vs GC/overhead.
- **Why not one giant executor?** I/O throughput tanks past ~5 threads and GC
  stalls on a huge heap; you also lose fault-tolerance granularity.
- **`spark.memory.fraction`?** 0.6 — unified execution+storage pool of (heap − 300 MB).
- **PySpark job killed for exceeding memory — why?** Python lives in **overhead**;
  raise `spark.executor.memoryOverhead`.
- **client vs cluster deploy mode?** Driver on the submitter vs inside the
  cluster; use cluster for production.
- **How do you set shuffle partitions and to what?** `spark.sql.shuffle.partitions`,
  ~2–3× total cores (or rely on AQE coalescing).
- **Client wants lower idle cost on a shared cluster?** Dynamic allocation with
  min/max executors + shuffle tracking.

---

**Next:** [12 · Debugging & the Spark UI](12-debugging-and-spark-ui.md) — find the
bottleneck before you tune it.
