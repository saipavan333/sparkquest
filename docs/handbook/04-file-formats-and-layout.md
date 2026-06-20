# 04 · File Formats & Physical Layout

> *Interview lens: "Why Parquet?" is a softball — but the follow-ups ("what's a row
> group?", "how does predicate pushdown actually work?", "partition vs bucket?")
> separate people who've *read* about Spark from people who've *operated* it.*

## 1. Row vs columnar storage

- **Row-oriented** (CSV, JSON, **Avro**) stores all fields of a record together.
  Great for writing whole records and for streaming, bad for analytics: to sum one
  column you read *every* column of *every* row.
- **Columnar** (**Parquet**, **ORC**) stores each column's values together. To sum
  one column you read only that column's bytes. Analytics queries touch few
  columns out of many, so columnar is a 10–100× I/O win — plus values of one type
  sit together, so they **compress** far better.

Rule: **Parquet/ORC for analytical tables; Avro for streaming/serialization and
schema-evolving message payloads; CSV/JSON only at the raw ingestion edge.**

## 2. Parquet internals (know this cold)

A Parquet file is a hierarchy:

```
File
 ├── Row Group 1   (a horizontal slice of rows, e.g. ~128 MB)
 │    ├── Column Chunk: user_id   → Pages (encoded + compressed)
 │    ├── Column Chunk: amount     → Pages
 │    └── ...  (+ per-column min/max/null statistics)
 ├── Row Group 2
 └── Footer  (schema + row-group metadata + stats + offsets)
```

- **Row group** — a batch of rows (default target ~128 MB). The unit of
  parallelism and of **predicate pushdown**: Parquet stores **min/max** per column
  per row group, so a filter `WHERE amount > 1000` lets the reader **skip entire
  row groups** whose `max(amount) ≤ 1000` without decoding them.
- **Column chunk → pages** — each column's data, split into pages, each
  independently **encoded** then **compressed**.
- **Encodings** — **dictionary encoding** (replace repeated values with small
  integer codes — huge for low-cardinality columns), **run-length encoding (RLE)**,
  **delta encoding**. These run *before* compression.
- **Compression** — **Snappy** (default, fast, moderate ratio) vs **Zstd/Gzip**
  (smaller, slower). Snappy is the usual analytics default.
- **Footer** — read **last**; holds the schema and all metadata/stats. This is why
  Parquet supports **column pruning** (read only the columns you select, by seeking
  to their chunk offsets) and **predicate pushdown** (use footer stats to skip).

So the two optimizations every interviewer wants by name:
**column pruning** (skip columns) + **predicate pushdown** (skip row groups).

**ORC** is conceptually the same with different names (file → **stripes** →
columns; built-in indexes, sometimes better compression; native to the Hive
world). **Avro** is row-based with a compact binary form and first-class **schema
evolution** — the standard for Kafka payloads.

## 3. Partitioning (directory layout)

`df.write.partitionBy("dt")` writes one **folder per value**:

```
/sales/dt=2026-01-01/part-*.parquet
/sales/dt=2026-01-02/part-*.parquet
```

A query `WHERE dt = '2026-01-01'` reads **only that folder** — **partition
pruning** — skipping the rest at the directory level (cheaper than row-group
skipping). Rules:

- Partition on **low-cardinality** columns you **filter on** (date, region,
  country). 
- **Never** partition on high-cardinality columns (user_id, timestamp-to-the-
  second) — you get millions of tiny folders/files (the small-files problem) and
  catastrophic metadata overhead.
- Aim for partitions of **at least ~1 GB**; sub-partition only if each is still
  large.

## 4. Bucketing (hash pre-shuffle)

`df.write.bucketBy(64, "user_id").sortBy("user_id").saveAsTable(...)` hashes rows
into a **fixed number of buckets** by key at write time. Later, a join/aggregation
on `user_id` between two tables bucketed the same way is **shuffle-free** — the
matching keys are already co-located. Tradeoffs:

- **Powerful** for repeated joins on the same high-cardinality key (where you
  *can't* partition).
- **Rigid** — the bucket count is fixed at write; changing it means a rewrite. Both
  sides must share bucket count to skip the shuffle.
- Spark's newer **Storage Partition Join (SPJ)** generalizes this idea to
  partitioned V2 sources (e.g. Iceberg) — same goal, avoid the exchange.

**Partition vs bucket in one line:** *partition = which files to read (pruning);
bucket = how rows are pre-shuffled within them (join/agg co-location).*

## 5. The small-files problem (a top operational interview topic)

Thousands of tiny files (a few KB–MB each) are a disaster: per-file open cost,
exploded metadata, slow listing on object stores, and on HDFS, NameNode memory
pressure. Causes: high shuffle-partition counts on write, over-partitioning,
streaming micro-batches each emitting files.

**Fixes:**
- `coalesce(n)` / `repartition(n)` before writing to control output file count.
- The **`REBALANCE`** hint (AQE) to size output partitions evenly.
- For Delta/Iceberg: **`OPTIMIZE`** (compaction) to merge small files into ~target-
  sized ones — see [chapter 05](05-lakehouse-and-delta.md).
- Target **~128 MB–1 GB per file**.

## 6. Putting it together — designing a table's layout

For a 10 TB events table queried mostly by date and user:
- Format **Parquet** (Snappy).
- **Partition by `dt`** (date) — most queries filter on a date range ⇒ partition
  pruning.
- **Bucket by `user_id`** if you frequently join on it ⇒ shuffle-free joins.
- Keep files ~256 MB–1 GB; schedule **compaction** to fight small files from
  incremental writes.
- Let Parquet stats + Spark do **column pruning + predicate pushdown** for free.

That layout answer — *format, partition, bucket, file size, compaction* — is
exactly what a "design the storage for X" question is fishing for.

---

**Next:** [05 · Lakehouse & Delta Lake](05-lakehouse-and-delta.md) — what you get
once the files become a *transactional table*.
