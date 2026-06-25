# 13 · Kafka & Streaming I/O

> *Interview lens: Kafka is the default source/sink for Structured Streaming.
> Expect "how do you read from Kafka, parse it, and guarantee exactly-once into
> your warehouse?" — and be ready to name where each guarantee actually comes
> from.*
>
> Prereqs: [03 · Streaming Internals](03-streaming-internals.md). Source of truth:
> the official
> [Structured Streaming + Kafka Integration guide](https://spark.apache.org/docs/3.5.1/structured-streaming-kafka-integration.html).

> **In plain words.** **Kafka** is a system that carries streams of events between
> applications — think of it as a never-ending conveyor belt of messages. This
> chapter shows how Spark reads from it, how to unpack each message (they arrive as
> raw text you turn into proper columns), how to write results back, and — the part
> interviewers care about — how to make sure every event is counted exactly once,
> even if something restarts.

## 1. Kafka in 90 seconds

- A **topic** is a named log, split into **partitions**. Ordering is guaranteed
  **only within a partition**, not across them.
- Each record has a monotonic **offset** within its partition. Consumers track
  "where am I" by offset.
- A **consumer group** splits a topic's partitions across its members; each
  partition is consumed by exactly one member of the group → horizontal scale.
- Records persist for a **retention** period (time/size), so consumers can
  **replay** — this replayability is what lets Spark recover exactly-once.

## 2. Reading from Kafka

```python
df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "host1:9092,host2:9092")
      .option("subscribe", "trades")            # or subscribePattern / assign
      .option("startingOffsets", "latest")      # earliest | latest | {json per TP}
      .option("maxOffsetsPerTrigger", 1_000_000) # rate limit / micro-batch sizing
      .load())
```

Every Kafka DataFrame has the **same fixed schema**, regardless of payload:

| column | type | meaning |
|---|---|---|
| `key` | binary | record key (often the partitioning id) |
| `value` | binary | the **payload** — your actual data |
| `topic` | string | source topic |
| `partition` | int | source partition |
| `offset` | long | position in the partition |
| `timestamp` | timestamp | record timestamp (often your event time) |
| `timestampType` | int | create-time vs append-time |

- **`subscribe`** a topic/list, **`subscribePattern`** a regex, or **`assign`**
  specific partitions.
- **`startingOffsets`**: `latest` (only new — typical for prod) or `earliest`
  (reprocess history); batch reads also take **`endingOffsets`**.
- **`failOnDataLoss`** (default `true`): fail if offsets vanish (aged out) — set
  `false` to skip gaps knowingly.

## 3. Parse the value (the universal first step)

`value` is **bytes**. Cast to string and apply a schema — almost always
`from_json`:

```python
from pyspark.sql import functions as F
schema = "ticker STRING, qty LONG, ts TIMESTAMP"
parsed = (df
    .select(F.col("value").cast("string").alias("json"), F.col("timestamp"))
    .select(F.from_json("json", schema).alias("d"), "timestamp")
    .select("d.*", "timestamp"))
```

(For Avro use `from_avro`; for Schema Registry, the Confluent deserializer.) This
parse step is exactly what lesson **st-07** drills.

## 4. Writing to Kafka

The sink reads specific columns: a **`value`** (required, string/binary) and
optional **`key`**, **`topic`**, **`partition`**, **`headers`**.

```python
(out.selectExpr("CAST(id AS STRING) AS key",
                "to_json(struct(*)) AS value")
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "host:9092")
    .option("topic", "enriched")
    .option("checkpointLocation", "/chk/enriched")   # REQUIRED
    .start())
```

`checkpointLocation` is mandatory — it stores the **offsets** that make recovery
exactly-once.

## 5. Flow control & sizing

- **`maxOffsetsPerTrigger`** — cap records per micro-batch (smooths spikes, sizes
  batches). The streaming cousin of partition sizing.
- **`minPartitions`** — split Kafka partitions into more Spark tasks for
  parallelism when you have few topic partitions but many cores.
- One **Spark task per Kafka partition** by default — so topic partition count is
  your read parallelism floor.

## 6. Delivery semantics — where exactly-once comes from

End-to-end **exactly-once** needs three things together:

1. A **replayable source** with trackable offsets → **Kafka** ✓.
2. **Checkpointing** → Spark records which offsets each micro-batch covered, in
   the checkpoint, atomically with progress (see [03 §5–6](03-streaming-internals.md)).
3. An **idempotent or transactional sink** → re-applying a replayed batch must
   not double-write.

Caveats interviewers reward:

- **Kafka as a *sink* is at-least-once** by default — on recovery a batch can be
  re-sent. Make downstream **idempotent** (dedupe by key/offset) or use a
  transactional write.
- **Files/Delta sinks** achieve exactly-once via Spark's atomic commit + the
  Delta transaction log.
- The classic production sink is **`foreachBatch` + Delta `MERGE`** (idempotent
  upsert), which gives you exactly-once even though `foreachBatch` is
  at-least-once at the batch level:

```python
def upsert(batch_df, batch_id):
    (DeltaTable.forName(spark, "trades")
        .alias("t").merge(batch_df.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

parsed.writeStream.foreachBatch(upsert) \
      .option("checkpointLocation", "/chk/trades").start()
```

## 7. Operational notes

- **Schema evolution**: a JSON/Avro payload changes — keep `from_json` tolerant
  (nullable fields), or enforce via Schema Registry.
- **Reprocessing**: to replay, start a **new checkpoint** with
  `startingOffsets=earliest` (reusing the old checkpoint resumes, doesn't replay).
- **Watermark + `dropDuplicates`** dedupes at-least-once delivery within a bounded
  window ([03 §3](03-streaming-internals.md)).

## 8. Rapid-fire interview Q&A

- **Schema of a Kafka source DataFrame?** key, value, topic, partition, offset,
  timestamp, timestampType — payload is in `value` (bytes).
- **First thing you do with `value`?** `CAST(value AS STRING)` then `from_json`
  with a schema.
- **`startingOffsets` earliest vs latest?** Reprocess all history vs only new
  records.
- **How is exactly-once achieved?** Replayable Kafka offsets + checkpoint +
  idempotent/transactional sink. All three, together.
- **Is writing to Kafka exactly-once?** No — at-least-once by default; dedupe
  downstream or write transactionally.
- **Required column to write to Kafka?** `value` (plus optional `key`/`topic`).
- **Control micro-batch size from Kafka?** `maxOffsetsPerTrigger`.
- **Read parallelism floor?** One task per Kafka partition (raise with
  `minPartitions`).
- **Production exactly-once sink to a lakehouse?** `foreachBatch` + Delta `MERGE`
  with a checkpoint.

---

**Next:** [05 · Lakehouse & Delta Lake](05-lakehouse-and-delta.md) and
[14 · Apache Iceberg](14-apache-iceberg.md) — where the stream lands.
