# 14 · Apache Iceberg

> *Interview lens: "Delta vs Iceberg vs Hudi — when would you pick each?" and "how
> does Iceberg do hidden partitioning and time travel?" are now standard senior
> data-engineering questions. Know the metadata tree.*
>
> Prereqs: [04 · File Formats & Layout](04-file-formats-and-layout.md),
> [05 · Lakehouse & Delta Lake](05-lakehouse-and-delta.md). Source of truth: the
> official [Iceberg docs](https://iceberg.apache.org/docs/latest/) and
> [Spark guide](https://iceberg.apache.org/docs/latest/spark-getting-started/).

## 1. The problem Iceberg solves

Classic **Hive tables** track data as "all files under these directories." That
breaks at scale: listing millions of files is slow, there's **no atomic commit**
(readers see half-written data), partition layout is baked into directory paths,
and schema/partition changes mean rewrites. **Iceberg** (and Delta, Hudi) replace
directory-listing with a **metadata layer** that brings ACID, snapshots, and safe
evolution to files on object storage.

## 2. The metadata tree (how Iceberg tracks a table)

Iceberg points a catalog entry at a chain of metadata, **not** a directory:

```
catalog → table metadata file (schema, partition spec, snapshots list)
            └─ snapshot → manifest list
                            └─ manifest files → data files (Parquet/ORC/Avro)
```

- A **snapshot** is the state of the table at one commit — a complete list of data
  files (via manifests). Every write creates a **new snapshot**; the old ones
  remain → **time travel** and **rollback** come for free.
- **Manifests** carry per-file stats (row counts, column min/max, null counts) so
  the planner **prunes files** without listing or opening them — fast scans on
  huge tables.
- A commit is an **atomic swap** of the current metadata pointer (optimistic
  concurrency) → ACID without locking the data files.

## 3. Hidden partitioning (the headline feature)

In Hive/Delta you partition by a *column you must also filter on* — and analysts
forget, scanning everything. Iceberg partitions by a **transform of a column** and
records it in metadata, so queries filter on the **raw** column and Iceberg
derives the partition automatically:

```sql
CREATE TABLE local.db.events (id BIGINT, ts TIMESTAMP, country STRING)
USING iceberg
PARTITIONED BY (days(ts), bucket(16, id));   -- transforms, not raw columns

SELECT * FROM local.db.events WHERE ts > '2026-01-01';  -- pruned automatically
```

Transforms: `years/months/days/hours(ts)`, `bucket(N, col)`, `truncate(N, col)`,
`identity`. The user never writes `WHERE day = ...`; that's "hidden."

## 4. Evolution without rewrites

- **Partition evolution** — change the partition spec (e.g. `days` → `hours`)
  going forward; **old data is not rewritten**. New writes use the new spec; reads
  span both. Impossible in Hive/Delta without a full rewrite.
- **Schema evolution** — add / drop / rename / reorder columns safely, because
  Iceberg tracks columns by **field id**, not by name or position. No accidental
  data corruption when a column is renamed.

## 5. Time travel & rollback

```sql
SELECT * FROM local.db.events VERSION AS OF 7         -- by snapshot id
SELECT * FROM local.db.events TIMESTAMP AS OF '2026-06-01 00:00:00';
-- inspect history
SELECT * FROM local.db.events.snapshots;
SELECT * FROM local.db.events.history;
-- roll back a bad write
CALL local.system.rollback_to_snapshot('local.db.events', 7);
```

## 6. Row-level changes: copy-on-write vs merge-on-read

Iceberg supports SQL `UPDATE`, `DELETE`, and `MERGE` — and lets you choose **how**
mutations are written:

- **Copy-on-write (CoW)** — rewrite the affected data files at write time. Slower
  writes, **fast reads** (no merge). Default; good for read-heavy tables.
- **Merge-on-read (MoR)** — write small **delete files** / position deletes and
  merge them at read time. **Fast writes**, slower reads until compaction. Good
  for frequent upserts / streaming CDC.

Set per table: `write.update.mode`, `write.delete.mode`, `write.merge.mode` =
`copy-on-write` | `merge-on-read`.

```sql
MERGE INTO local.db.trades t USING updates s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.qty = s.qty
WHEN NOT MATCHED THEN INSERT *;
```

## 7. Table maintenance (operational must-knows)

Snapshots and small files accumulate; you run maintenance procedures:

- **`expire_snapshots`** — drop old snapshots + their now-unreferenced files
  (reclaims storage, ends time travel past that point).
- **`rewrite_data_files`** — **compaction**: combine small files into right-sized
  ones (the small-files problem from streaming writes).
- **`rewrite_manifests`** — optimize the manifest layer for planning speed.
- **`remove_orphan_files`** — delete files no snapshot references.

```sql
CALL local.system.rewrite_data_files('local.db.trades');
CALL local.system.expire_snapshots('local.db.trades', TIMESTAMP '2026-06-01 00:00:00');
```

## 8. Delta vs Iceberg vs Hudi

| | **Delta Lake** | **Apache Iceberg** | **Apache Hudi** |
|---|---|---|---|
| Origin | Databricks | Netflix | Uber |
| Metadata | transaction log (`_delta_log` JSON + Parquet checkpoints) | snapshot/manifest tree | timeline + file groups |
| Engine reach | Spark-first (broadening) | **engine-agnostic** (Spark, Flink, Trino, Presto, Dremio) | Spark/Flink |
| Partitioning | directory (like Hive) | **hidden + evolvable** | directory |
| Schema evolution | yes | **by field id (safest)** | yes |
| Sweet spot | Databricks/Spark lakehouse, `OPTIMIZE`+Z-order | huge tables, many engines, evolving layout | **record-level upserts / incremental CDC** |

> **Interview answer:** "All three give ACID, time travel, and upserts on object
> storage. I'd pick **Delta** in a Spark/Databricks shop, **Iceberg** when
> multiple engines query the same tables or I need partition evolution, and
> **Hudi** for heavy streaming record-level upserts."

## 9. Rapid-fire interview Q&A

- **What is a snapshot?** The complete file list of the table at one commit; new
  one per write → time travel/rollback.
- **How does Iceberg prune files?** Per-file min/max/null stats in manifests — no
  directory listing.
- **Hidden partitioning?** Partition by a *transform* recorded in metadata; queries
  filter the raw column and Iceberg derives the partition.
- **Partition evolution — does it rewrite old data?** No; new spec applies going
  forward, reads span both.
- **CoW vs MoR?** Rewrite files now (fast reads) vs write delete files and merge on
  read (fast writes); choose by read/write ratio.
- **Why field ids for schema evolution?** Renames/reorders can't corrupt data —
  columns are tracked by id, not name/position.
- **Maintenance you'd schedule?** `rewrite_data_files` (compaction) +
  `expire_snapshots` (storage + metadata hygiene).
- **Delta vs Iceberg?** Spark-first + Z-order vs engine-agnostic + hidden/evolving
  partitioning.

---

**Next:** [06 · Data Modeling](06-data-modeling.md) and
[07 · Pipeline System Design](07-system-design.md) — turning tables into systems.
