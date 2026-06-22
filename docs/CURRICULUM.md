# SparkQuest Curriculum — the one-stop guide to Python, PySpark & Spark Streaming for Data Engineers

A complete, hands-on path from absolute beginner to job-ready data engineer. Every
topic below maps to a **graded, runnable challenge** (✅ = live now, 🔜 = on the
roadmap). The structure mirrors the official documentation so you can always go
deeper at the source.

**Status:** 85 graded lessons across all 7 tracks. The 76 Python / PySpark /
Performance / Streaming / Capstone lessons are verified in the offline sandbox; the
9 Delta + Iceberg lessons are verified by a dedicated Maven-enabled CI job (both
need an internet JAR fetch). **Every lesson ships read-along teaching notes** in its
brief — concept, why it matters, gotchas, and the interview angle, with cross-links
into the Handbook — across all 7 tracks, so each lesson teaches as you practice. Every reference solution passes its own auto-grader.
Pair the lessons with the **[Handbook](handbook/)** — now **15 deep-dive chapters**
(incl. joins & AQE, RDDs, config & cluster sizing, debugging & the Spark UI, Kafka,
Iceberg, and testing & data quality) — and the in-app **🎤 Mock Interview** drill
with a **timed, scored exam mode** over a **201-question bank**.

**Primary references (always current):**
- Python — <https://docs.python.org/3/tutorial/> · <https://docs.python.org/3/library/>
- Spark SQL & DataFrames — <https://spark.apache.org/docs/latest/sql-programming-guide.html>
- PySpark API — <https://spark.apache.org/docs/latest/api/python/index.html>
- DataFrame functions — <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html>
- Structured Streaming — <https://spark.apache.org/docs/latest/streaming/index.html>
- Performance tuning — <https://spark.apache.org/docs/latest/sql-performance-tuning.html>
- Delta Lake — <https://docs.delta.io/latest/index.html>
- Apache Iceberg — <https://iceberg.apache.org/docs/latest/spark-getting-started/>

How it's taught: every challenge gives you a brief, starter code, and an AI tutor;
you write real code, it executes against a real engine, and an auto-grader checks
your output. Tracks are ordered — finish one before the next.

> **For interview-grade depth**, pair these lessons with the **[SparkQuest
> Handbook](handbook/)** — book-quality deep dives on internals, performance, and
> streaming, plus an interview question bank with model answers.

---

## Track 1 — Python for Data Engineering (22 live)

> *Goal: write clean, idiomatic Python that's the backbone of every pipeline.*
> Docs: <https://docs.python.org/3/tutorial/>

**Module 1.1 — Core syntax**
- ✅ Print & strings — output, f-strings, quoting
- ✅ Variables & arithmetic — numeric types, operators
- ✅ Lists & loops — sequences, iteration, accumulation
- ✅ Dictionaries — key/value lookups, the basis of joins & caches
- ✅ Functions — parameters, return values, reuse
- ✅ Word count — counting with dicts (the MapReduce intuition)

**Module 1.2 — Idiomatic Python**
- ✅ Comprehensions — list/dict comprehensions, filtering
- ✅ Error handling — `try`/`except`, robustness
- ✅ Sets, tuples & unpacking — uniqueness, immutability, star-unpacking
- ✅ `*args` / `**kwargs` — flexible function signatures
- ✅ Lambdas & functional style — anonymous functions as `sorted`/`max`/`filter`/`map`/`reduce` keys, lambda vs `def`

**Module 1.3 — Data-engineering Python**
- ✅ Generators & iterators — `yield`, lazy streams, memory efficiency
- ✅ Decorators — wrapping behaviour (timing, retry, logging)
- ✅ Dataclasses & OOP — `@dataclass`, classes, methods
- ✅ `collections` & `itertools` — `Counter`, `defaultdict`, `groupby`
- ✅ Dates & times — `datetime`, `strptime`, extracting parts
- ✅ JSON — `json.loads`/`dumps`, parsing records
- ✅ Context managers — `with`, `__enter__`/`__exit__`, guaranteed cleanup
- ✅ Type hints — `typing`, `Optional`, annotations, why DE teams enforce them
- ✅ Files & `pathlib` — `Path`, stem/suffix/parent, cross-platform paths
- ✅ CSV — the `csv` module / `DictReader`, why values are strings
- ✅ Logging — levels, handlers, structured logs over `print`
- 🔜 Testing — `assert`, designing for testability (see Handbook ch.15)

---

## Track 2 — PySpark Foundations & DataFrames (34 live)

> *Goal: think in distributed DataFrames; read, transform, and write any data.*
> Docs: <https://spark.apache.org/docs/latest/sql-getting-started.html>

**Module 2.1 — Getting started**
- ✅ Your first DataFrame — `createDataFrame`, rows & columns
- ✅ Explicit schemas — `StructType`/`StructField`, declared types
- ✅ Reading & writing files — Parquet round-trip (read/write/modes)
- 🔜 SparkSession & architecture — driver/executors, lazy evaluation, the DAG
- 🔜 RDD basics — `map`/`filter`/`reduce`, when low-level still matters

**Module 2.2 — Core transformations**
- ✅ Select & filter — projection and row filtering
- ✅ Derive columns — `withColumn`, expressions
- ✅ Conditionals — `when`/`otherwise`
- ✅ String functions — `upper`, `concat_ws`, and friends
- ✅ Date/time functions — `to_date`, `datediff`, `year`
- ✅ Null handling — `na.fill`/`na.drop`
- ✅ Casting types — `cast`, schema enforcement
- 🔜 Math & rounding — `round`, `abs`
- 🔜 Sort, distinct & dedup — `orderBy`, `dropDuplicates`
- 🔜 Union & set ops — `unionByName`, `subtract`

**Module 2.3 — Aggregations & joins**
- ✅ Group & aggregate — `groupBy().agg()`
- ✅ Joins — inner join on a key
- ✅ All join types — left, `left_anti` (finding unmatched rows)
- ✅ Pivot — `groupBy().pivot()`
- ✅ Spark SQL — temp views, `spark.sql(...)`
- ✅ Window functions — `row_number`, ranking
- ✅ More windows — `rank`/`dense_rank`, `lag`/`lead`, **running totals**, **sessionization** (gaps & islands)
- ✅ Set operations — `unionByName`, `exceptAll`, `intersect`
- ✅ `selectExpr` & SQL expression strings
- ✅ Spark SQL analytics — top-N per group with `ROW_NUMBER()` window in SQL
- ✅ Data quality — validate & quarantine rows (don't `dropna()` silently)

**Module 2.4 — Complex & semi-structured data**
- ✅ Arrays — `explode`, flattening
- ✅ Structs — nested columns, dot access
- ✅ UDFs — custom logic (and why to prefer built-ins)
- ✅ Maps — key/value access by key
- ✅ pandas UDFs — vectorised UDFs with Arrow (`@pandas_udf`)

**Module 2.5 — The RDD layer (low-level API, interview staple)**
- ✅ RDD basics — `parallelize`, `map`, `filter`, `collect` (lazy + actions)
- ✅ `reduceByKey` — pair RDDs, map-side combine, the word-count pattern
- ✅ Broadcast variables — ship a read-only lookup once per executor
- ✅ `mapPartitions` — amortise per-partition setup (connections, models)
- 🔜 Accumulators — add-only metrics (and the exactly-once caveat)

> Deep dive: [Handbook ch.10 — RDDs & the Low-Level API](handbook/10-rdd-and-low-level-api.md).

---

## Track 3 — Performance & Internals (7 live)

> *Goal: understand what Spark does under the hood and make it fast.*
> Docs: <https://spark.apache.org/docs/latest/sql-performance-tuning.html>

- ✅ Repartition vs coalesce — controlling partitions
- ✅ Caching & persistence — `cache`/`persist`, storage levels
- ✅ Broadcast joins — `broadcast()`, avoiding shuffles
- ✅ Data skew & salting — spreading hot keys
- ✅ Partitioned writes — `partitionBy` on write
- ✅ Partition pruning — filter a partition column, read only matching dirs
- ✅ Bucketing — write-time hash layout for shuffle-free joins (`bucketBy`)
- ✅ Catalyst & Adaptive Query Execution — *deep dive:* [Handbook ch.9](handbook/09-joins-shuffle-aqe.md)
- ✅ Reading the Spark UI — stages, tasks, shuffle, spill, skew — [Handbook ch.12](handbook/12-debugging-and-spark-ui.md)
- ✅ Cluster sizing & config — executors/cores/memory math — [Handbook ch.11](handbook/11-configuration-and-cluster-sizing.md)

> Deep dives: [ch.9 Joins & AQE](handbook/09-joins-shuffle-aqe.md) ·
> [ch.11 Config & Sizing](handbook/11-configuration-and-cluster-sizing.md) ·
> [ch.12 Debugging & Spark UI](handbook/12-debugging-and-spark-ui.md).

---

## Track 4 — Structured Streaming (8 live)

> *Goal: build real-time pipelines with the same DataFrame API.*
> Docs: <https://spark.apache.org/docs/latest/streaming/index.html>

- ✅ Your first stream — `readStream`/`writeStream`, memory sink, `AvailableNow`
- ✅ Streaming aggregation — running counts, `complete` mode
- ✅ Event-time windows & watermarks — tumbling windows, late data
- ✅ Streaming deduplication — `dropDuplicates` with a watermark
- ✅ Custom sinks with `foreachBatch` — per-batch DataFrames, upserts
- ✅ Stream-static joins — enriching a stream with reference data
- ✅ Parsing a Kafka value — `from_json` over the `value` column (the universal step)
- ✅ Stream-stream joins — joining two live streams with watermarks + time bounds
- 🔜 Output modes & triggers — append/update/complete, `processingTime`
- 🔜 Sliding & session windows — overlapping and gap-based windows

> Deep dives: [ch.3 Streaming Internals](handbook/03-streaming-internals.md) ·
> [ch.13 Kafka & Streaming I/O](handbook/13-kafka-and-streaming-io.md) (offsets,
> exactly-once, `foreachBatch`+`MERGE`).

---

## Track 5 — Lakehouse & Delta Lake (4 live)

> *Goal: bring ACID, upserts, and time travel to data lakes.*
> Docs: <https://docs.delta.io/latest/delta-intro.html>
> *Note: Delta needs the `delta-spark` package + a Maven-fetched JAR, so these
> lessons run in CI / Docker / your machine (which have Maven). They're verified by
> a dedicated `delta` CI job, isolated so it can never block the main check.*

- ✅ Delta basics — create a Delta table, ACID write/read
- ✅ `MERGE` / upserts — change-data-capture into a table
- ✅ Time travel — querying previous versions (`versionAsOf`)
- ✅ Schema evolution — `mergeSchema` on append
- 🔜 Parquet deep-dive — columnar storage, predicate/column pushdown
- 🔜 Medallion architecture — bronze → silver → gold layers

---

## Track 6 — Apache Iceberg (5 live)

> *Goal: the other open table format senior interviews ask about — know how it
> compares to Delta.*
> Docs: <https://iceberg.apache.org/docs/latest/spark-getting-started/>
> *Note: like Delta, Iceberg needs a Maven-fetched runtime JAR, so these lessons run
> in CI / Docker / your machine. They're verified by the same Maven-enabled CI job,
> isolated so it can never block the main check.*

- ✅ Your first Iceberg table — `CREATE TABLE … USING iceberg`, insert, read back
- ✅ Row-level `UPDATE` — copy-on-write mutation of a single row via SQL
- ✅ `MERGE` / upsert (CDC) — update matches, insert non-matches atomically
- ✅ Time travel & snapshots — read an earlier `snapshot-id` / `VERSION AS OF`
- ✅ Hidden partitioning — `bucket(4, ticker)` transform, query the raw column

> Deep dive: [Handbook ch.14 — Apache Iceberg](handbook/14-apache-iceberg.md)
> (metadata tree, hidden partitioning, CoW vs MoR, Delta vs Iceberg vs Hudi).

---

## Track 7 — Capstone ETL Projects (5 live)

> *Goal: combine everything into production-shaped pipelines.*

- ✅ Batch ETL pipeline — ingest → clean → enrich → aggregate
- ✅ Data-quality checks — valid vs quarantined rows, null/range assertions
- ✅ Streaming pipeline capstone — filter → enrich (join) → aggregate on a live stream
- ✅ Slowly Changing Dimension (Type 2) — expire old rows, insert new current rows
- ✅ Incremental processing — high-water mark, idempotent reruns, backfills
- 🔜 Orchestration — scheduling and dependencies (concept)

> Deep dives: [ch.6 Data Modeling](handbook/06-data-modeling.md) (SCD 1/2/3) ·
> [ch.15 Testing & Data Quality](handbook/15-testing-and-data-quality.md)
> (unit tests, the six DQ dimensions, the quarantine pattern, frameworks).

---

### Phased rollout

This is a living curriculum. **Phase 1** shipped the full syllabus plus Modules
1.1–1.3 and the core of Track 2. **Phase 2** added the remaining Python stdlib
topics, advanced DataFrame ops (joins, windows, Parquet, UDFs, structs),
partitioned writes, and the entire Capstone track. **Phase 3** added the Delta Lake
track (create, MERGE, time travel, schema evolution) plus advanced PySpark
windowing/sessionization and an in-app **Mock Interview** — **61 graded lessons total**.
Remaining 🔜 topics (AQE, Spark UI, stream-stream joins, medallion, CDC) land in
future phases. Track your progress in the app — solved counts and badges update per
track.
