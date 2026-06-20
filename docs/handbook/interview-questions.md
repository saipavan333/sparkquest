# Interview Question Bank

Model answers are deliberately **concise** — say them in your own words and be
ready for the follow-up ("why?", "how would you scale that?"). Grouped by round.

---

## A. Spark conceptual (the warm-ups)

**1. Transformation vs action?**
Transformations (`select`, `filter`, `join`) are lazy — they build lineage and run
nothing. Actions (`count`, `collect`, `write`) trigger a job that plans and executes
the lineage. Laziness lets Catalyst optimize the whole query.

**2. Narrow vs wide transformation?**
Narrow: output partition depends on one input partition, no data movement (`map`,
`filter`). Wide: depends on many, requires a **shuffle** across the network
(`groupBy`, `join`, `distinct`). Stage boundaries fall on shuffles.

**3. What is a shuffle and why is it expensive?**
Repartitioning data by key across the cluster. Costs disk (map-side spill files),
network (reduce-side fetch), serialization, and is where skew/spill bite. Minimize
it: broadcast small joins, pre-bucket, reduce wide ops.

**4. How does a job become tasks?**
Action → job → DAG scheduler splits at shuffles into **stages** → each stage = one
**task per partition** → task scheduler ships tasks to executor cores, honoring data
locality.

**5. RDD vs DataFrame — which and why?**
DataFrame, almost always: Catalyst optimizes it and Tungsten executes it in compact
binary memory. RDDs are opaque to the optimizer and, in PySpark, pay a JVM↔Python
serialization tax. Drop to RDDs only for logic the DataFrame API can't express.

**6. What does Catalyst do?**
Four phases: analysis (resolve names/types), logical optimization (predicate
pushdown, column pruning, join reorder), physical planning (pick join strategy via
cost model), and code generation (Tungsten whole-stage codegen).

**7. Predicate pushdown & column pruning?**
With columnar sources (Parquet), Spark reads only needed columns and skips row
groups whose min/max stats can't satisfy the filter — often 10–100× less I/O.

**8. `repartition` vs `coalesce`?**
`repartition(n)` = full shuffle to exactly `n` (or by column); use to increase or
redistribute. `coalesce(n)` = merge to ≤ n with **no shuffle**; use to reduce
partitions cheaply (e.g. before writing).

**9. `cache()` — when and when not?**
Cache a DataFrame you **reuse** across actions; it also cuts lineage so you don't
recompute from source each time. Don't cache single-use data — it wastes memory and
can evict useful blocks. `unpersist()` when done.

**10. The four join strategies, and how Spark chooses?**
Broadcast hash (small side < 10 MB, no shuffle — fastest), sort-merge (two large
tables), shuffle hash (one side fits in memory), broadcast nested loop (non-equi,
tiny side). Auto-broadcast uses stats; hint priority BROADCAST > MERGE >
SHUFFLE_HASH > SHUFFLE_REPLICATE_NL.

**11. What is AQE and what does it fix?**
Adaptive Query Execution (default since 3.2) re-optimizes mid-query from real
runtime stats: coalesces shuffle partitions, switches sort-merge→broadcast when a
side is small, and splits skewed partitions. It fixes the "200-partition default"
and "stale statistics" problems automatically.

**12. `groupBy().count()` vs `groupBy().collect_list()` at scale?**
`count`/`sum` do a **map-side combine** (partial aggregate before shuffle), so they
ship little data. `collect_list` has no combine — it shuffles every row and can OOM
on a skewed key.

**13. Why did my job OOM on the driver?**
Almost always `collect()`/`toPandas()` on a big result, or broadcasting a too-large
table. The driver isn't sized for data. Use `write`, `take(n)`, or aggregate first.

---

## B. Data skew (expect a whole sub-interview)

**14. You see one task taking 100× longer than the rest. Diagnose & fix.**
That's **skew** — one key holds most rows. In order: (1) filter junk/`NULL` keys if
not needed; (2) broadcast the other side if small; (3) rely on **AQE skew join**
(splits skewed partitions automatically); (4) **salt** the hot key — add a random
`0..N-1` to the big side's key, explode the small side across all N salts, join on
`(key, salt)`, aggregate the salt away; (5) two-phase (salted partial → final)
aggregation.

**15. Why does salting work?**
It turns one hot partition into N partitions processed by N tasks in parallel,
trading a slightly larger small side (replicated N×) for balanced reduce tasks.

---

## C. Structured Streaming

**16. Event time vs processing time — why care?**
Event time = when it happened (in the data); processing time = when Spark saw it.
Late/out-of-order arrival means processing-time windows are wrong. Aggregate by
event time.

**17. What is a watermark and what breaks without one?**
A moving "accept events up to T late" threshold. It bounds **state**: without it,
Spark keeps every window's state forever (memory leak → job death) and can't
finalize append-mode results. It also drops too-late events.

**18. How does Spark achieve exactly-once?**
Replayable source (Kafka offsets / files) + checkpointed offsets (write-ahead) +
idempotent/transactional sink. Spark guarantees replay and state recovery; the sink
must dedupe — canonically `foreachBatch` + Delta `MERGE` on a unique key.

**19. Append vs update vs complete output mode?**
Append: only finalized new rows (needs watermark for aggregations). Update: only
changed rows. Complete: whole result table each batch (small aggregations only;
never forgets state).

**20. Stream-stream join — what's the catch?**
Both sides buffer in state until matches arrive, so you must watermark both sides
and time-bound the join, or state grows unbounded. Prefer stream-**static** joins
(stateless) when enriching with reference data.

**21. HDFS vs RocksDB state store?**
Default HDFS-backed store keeps state in JVM heap → GC pressure / OOM at scale.
RocksDB store keeps it off-heap on local disk → far larger state with stable GC.
Use RocksDB for big stateful jobs.

---

## D. PySpark / SQL coding patterns

> Interviewers test whether you reach for the *right* primitive. Approaches, not
> just answers.

**22. Top-N per group (e.g. top 3 trades per ticker).**
`Window.partitionBy("ticker").orderBy(F.desc("amount"))` + `row_number()`, filter
`rk <= 3`. (Use `rank`/`dense_rank` if ties should share a rank.) Lessons:
`sp-07`, `sp-17`.

**23. Deduplicate keeping the latest record per key.**
`row_number()` over `partitionBy(key).orderBy(desc(updated_at))`, keep `rk == 1`.
In streaming, `dropDuplicates` + watermark (`st-04`).

**24. Sessionization / gaps-and-islands.**
`lag` the timestamp per user; mark a new session when gap > threshold
(`F.when(ts - lag(ts) > gap, 1).otherwise(0)`), cumulative-sum the flags into a
session id over an ordered window.

**25. Running / cumulative total.**
`F.sum("amt").over(Window.partitionBy(k).orderBy(t).rowsBetween(Window.unbounded
Preceding, Window.currentRow))`.

**26. Pivot side into columns.** `groupBy("ticker").pivot("side").sum("qty")`
(`sp-15`).

**27. Explode a JSON/array column.** `from_json` with a schema, then `F.explode`
(`sp-14`).

**28. Word count (the "hello world" of MapReduce).**
`df.select(explode(split(line, " ")).alias("w")).groupBy("w").count()`. Be ready to
explain the map-side combine.

**29. SCD Type 2 upsert into a dimension.**
Delta `MERGE`: when matched and attributes changed, close the current row
(`end_date`, `is_current=false`) and insert a new current version; when not
matched, insert. Key on the business key + `is_current` (`delta-02` shows the
MERGE mechanics).

**30. Find duplicates / counts > 1.** `groupBy(cols).count().filter("count > 1")`.

---

## E. System design (the senior round)

> Use one framework every time: **Requirements → Data model → Ingestion →
> Processing → Storage → Serving → Quality/Ops → Tradeoffs.**

**31. "Design a pipeline to compute daily active users from clickstream."**
Clarify scale/latency/SLA. Ingest events to **Kafka**. Decide batch vs streaming:
DAU is daily ⇒ **incremental batch** (or streaming with daily windows). Land raw
events as **bronze** Parquet/Delta partitioned by date; clean/dedup to **silver**;
aggregate distinct users per day to **gold**. Handle **late events** with a
watermark / a look-back window and idempotent `MERGE` so re-runs don't double-count.
Serve gold to a warehouse/BI. Add **data-quality** checks (row counts, null rates)
and **orchestration** (Airflow DAG with retries + SLAs). Tradeoffs: exactness of
distinct counts (HyperLogLog approx vs exact), cost vs freshness.

**32. "Lambda vs Kappa?"**
Lambda = parallel batch (accurate, slow) + speed (approximate, fast) layers merged
at serving — robust but you maintain two codebases. Kappa = a single streaming
pipeline; reprocess by replaying the log. Modern lakehouses lean Kappa-ish because
streaming + Delta give both. Choose by whether you truly need a separate batch
truth layer.

**33. "How do you make a pipeline idempotent / safe to re-run (backfills)?"**
Make writes **upserts** keyed by a deterministic business/event key (Delta
`MERGE`), partition by the processing date so a re-run overwrites exactly one
partition, record processed offsets/files (checkpoint or a control table), and
avoid `append` of non-deduped data. Then a backfill is "delete the date partition,
re-run."

**34. "A 2 TB / 200 GB join is slow. Walk me through it."**
Open the UI → is it skew (task-time spread) or just a huge shuffle? If one side is
small-ish, **broadcast** it (or hint it) to kill the shuffle. If skewed, lean on
AQE skew join or **salt**. If both are large and joined repeatedly, **bucket** both
on the key at write time so future joins skip the shuffle. Ensure stats exist so the
optimizer/AQE choose well.

---

## F. Python for data engineering

**35. Generators vs lists — why do DE pipelines love generators?**
Generators yield lazily, holding one item at a time → constant memory over huge/
infinite streams. Lists materialize everything. (Mirrors Spark's lazy model.)

**36. Threading vs multiprocessing vs asyncio (and the GIL)?**
The **GIL** lets only one thread run Python bytecode at a time, so **threads** help
only **I/O-bound** work (they release the GIL on I/O). **multiprocessing** uses
separate processes → true parallelism for **CPU-bound** work. **asyncio** is single-
threaded cooperative concurrency for high-I/O fan-out (thousands of sockets). DE
ingestion (API/DB calls) is usually I/O-bound ⇒ threads/asyncio.

**37. What's a decorator and a real DE use?**
A function that wraps another to add behavior without changing it — retry-with-
backoff on a flaky API call, timing/logging a task, caching. (`py-10`.)

**38. How do you test data pipelines?**
Unit-test pure transformation functions on tiny DataFrames with known expected
output (exactly what SparkQuest's auto-grader does); assert schema and row-level
results; add data-quality assertions (counts, null/range) as runtime gates; keep
logic in small, mockable functions.

---

*Drill these aloud. The lessons referenced (e.g. `sp-17`, `delta-02`, `st-04`)
let you **run** the pattern, not just read it.*
