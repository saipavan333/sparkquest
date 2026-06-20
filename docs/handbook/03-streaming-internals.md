# 03 · Structured Streaming Internals

> *Interview lens: anyone can call `readStream`. Seniors are tested on **event
> time vs processing time, watermarks, state, checkpointing, and exactly-once** —
> the correctness machinery. Get these right and you're in rare company.*

## 1. The core idea: a stream is an unbounded table

Structured Streaming models a stream as a **table that grows forever**. Your query
is written exactly like a batch query; Spark incrementally maintains the result as
new rows "append" to the input table. This is the whole reason the DataFrame API
is identical for batch and streaming — *write once, run continuously.*

Execution is **micro-batch** by default: each **trigger**, Spark processes the new
data as a small batch, updates results and state, and commits. (A separate,
experimental **Continuous Processing** mode offers ~1 ms latency with at-least-once
semantics, but micro-batch — with ~100 ms–seconds latency and **exactly-once** — is
what you'll use and discuss.)

## 2. Event time vs processing time (the distinction interviews hinge on)

- **Processing time** — when Spark *sees* the record. Easy, but wrong for
  analytics: a network blip makes "events per minute" meaningless.
- **Event time** — when the event *actually happened* (a timestamp in the data).
  Correct, but events arrive **late and out of order** (a phone was offline; a
  Kafka partition lagged).

Windowed aggregations should be by **event time** (`F.window("event_ts", "10
minutes")`). The challenge event time creates: *how long do you wait for late
events before finalizing a window?* That's what watermarks answer.

## 3. Watermarks — bounding lateness and state

A **watermark** is a moving threshold: *"I will accept events up to T late; older
ones are dropped."* You declare it with `withWatermark("event_ts", "10 minutes")`.

```
watermark = max event time seen so far − allowed lateness
```

It does two jobs:

1. **Correctness with late data** — events later than the watermark are dropped
   (in append mode) instead of corrupting an already-emitted window.
2. **Bounding state (the real reason)** — without a watermark, Spark would keep
   **every** window's state forever to handle arbitrarily late data → unbounded
   memory → the job dies. The watermark lets Spark **drop state** for windows that
   can no longer receive valid events. *No watermark on a streaming aggregation =
   a state leak.* This is the #1 thing to say.

**Append mode + aggregation requires a watermark**, because append only emits a
window's result *once it's final* — and "final" is defined by the watermark.

## 4. State and the state store

Stateful operations — aggregations, dedup, stream-stream joins, `[flat]
MapGroupsWithState` — keep **state** between micro-batches (e.g. the running count
per key, or buffered rows for a join). That state lives in the **state store**,
keyed by partition, and is **checkpointed** every batch.

- **Default (HDFS-backed) state store** — keeps state in executor JVM memory,
  backed by checkpoint files. Simple, but large state ⇒ GC pressure and OOM.
- **RocksDB state store** (`spark.sql.streaming.stateStore.providerClass` =
  RocksDB provider) — keeps state in an embedded RocksDB (off-heap + local disk),
  so you can hold **far more state** with stable GC. The standard choice for
  large stateful streaming (millions of keys, big join buffers).

State is **partitioned** by `spark.sql.shuffle.partitions` at the time the query
*first* starts — and you **cannot change it later** without losing state, a classic
production footgun.

## 5. Checkpointing & fault tolerance

A streaming query's **checkpoint location** is its source of truth for recovery.
It contains:

- **Offset log (WAL)** — which input offsets belong to each batch, written
  *before* processing (write-ahead). On restart, Spark knows exactly where to
  resume.
- **State store files** — the snapshotted/delta'd state per batch.
- **Commit log** — which batches finished successfully.

On failure, Spark restarts from the last committed batch, **replays** the source
from the recorded offsets, and restores state. The checkpoint is tied to the
query's plan — **don't delete it, and don't change the query shape** (it'll refuse
to resume).

## 6. Exactly-once — how it's actually achieved

Spark gives **exactly-once** end-to-end **only if** three things hold:

1. **Replayable source** — can re-read from a recorded offset after failure.
   **Kafka** (offsets) and the **file source** qualify; a socket does not.
2. **Deterministic computation** — same input → same output.
3. **Idempotent or transactional sink** — re-writing the same batch produces no
   duplicates. The built-in **file sink** uses a manifest/commit protocol; for
   anything else, use **`foreachBatch`** and make the write idempotent (e.g. a
   Delta **`MERGE`** keyed on a unique id, or an upsert keyed by `(partition,
   batch_id)`).

Say it crisply: *"Exactly-once = replayable source + idempotent/transactional sink
+ checkpointed offsets. Spark guarantees its half; the sink must guarantee the
other half — which is why `foreachBatch` + Delta `MERGE` is the canonical exactly-
once sink."*

## 7. Output modes (and which operations allow them)

- **Append** — only new, *finalized* rows are emitted. Default; required for most
  sinks. For aggregations, needs a watermark (so rows can be finalized).
- **Update** — only rows that *changed* this batch are emitted. Good for upserting
  running aggregates.
- **Complete** — the **entire** result table is re-emitted every batch. Only for
  aggregations, and only when the result is small (it never forgets state).

## 8. Triggers

- **Default** (unspecified) — fire a micro-batch as soon as the previous finishes.
- **`processingTime="30 seconds"`** — fixed wall-clock cadence.
- **`availableNow=True`** (or the older `once=True`) — process **all** currently
  available data, then **stop**. Perfect for: cost-efficient incremental batch
  jobs, *and* deterministic testing/grading (every SparkQuest streaming lesson uses
  it).
- **`continuous="1 second"`** — experimental low-latency mode (at-least-once).

## 9. Stream–stream joins (advanced, frequently asked)

Joining two streams requires **buffering both sides in state** until matches can
arrive — so you **must** set watermarks on both sides *and* a time-bound on the
join condition (e.g. "within 1 hour"), or state grows forever. Inner joins emit on
match; **outer** joins additionally emit unmatched rows once the watermark
guarantees no match can still come. Stream–**static** joins (enrich a stream with a
reference table) are stateless and need no watermark — much simpler, and usually
the right tool.

## 10. Kafka integration (the default real source)

- Read with `readStream.format("kafka")`, `startingOffsets` (`earliest`/`latest`/
  explicit), and the value is `binary` — you `cast("string")` then parse
  (`from_json`).
- **Backpressure / throughput:** `maxOffsetsPerTrigger` caps records per batch so a
  backlog doesn't create one monster batch.
- **Offsets are managed by Spark in the checkpoint**, *not* committed back to
  Kafka consumer groups — that's how it gets exactly-once replay (don't rely on
  Kafka's own offset tracking).
- Write back with `writeStream.format("kafka")`; for exactly-once to Kafka,
  dedupe downstream or use idempotent producers + keys.

---

**Bringing it home:** the streaming lessons (`st-*`, `cap-03`) give you the muscle
memory — file source, `availableNow`, windowed aggregation, watermark, dedup,
`foreachBatch`, stream-static join. This chapter is the *why* an interviewer will
chase. Pair them.

**Next:** the [Interview Question Bank](interview-questions.md).
