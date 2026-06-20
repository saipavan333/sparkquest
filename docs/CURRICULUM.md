# SparkQuest Curriculum — the one-stop guide to Python, PySpark & Spark Streaming for Data Engineers

A complete, hands-on path from absolute beginner to job-ready data engineer. Every
topic below maps to a **graded, runnable challenge** (✅ = live now, 🔜 = being
added in the phased rollout). The structure mirrors the official documentation so
you can always go deeper at the source.

**Primary references (always current):**
- Python — <https://docs.python.org/3/tutorial/> · <https://docs.python.org/3/library/>
- Spark SQL & DataFrames — <https://spark.apache.org/docs/latest/sql-programming-guide.html>
- PySpark API — <https://spark.apache.org/docs/latest/api/python/index.html>
- DataFrame functions — <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html>
- Structured Streaming — <https://spark.apache.org/docs/latest/streaming/index.html>
- Performance tuning — <https://spark.apache.org/docs/latest/sql-performance-tuning.html>
- Delta Lake — <https://docs.delta.io/latest/index.html>

How it's taught: every challenge gives you a brief, starter code, and an AI tutor;
you write real code, it executes against a real engine, and an auto-grader checks
your output. Tracks are ordered — finish one before the next.

---

## Track 1 — Python for Data Engineering

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
- ✅ Comprehensions — list/dict/set comprehensions, filtering
- ✅ Error handling — `try`/`except`/`finally`, raising, custom exceptions
- 🔜 Tuples, sets & unpacking — immutability, membership, multiple assignment
- 🔜 `*args` / `**kwargs` & lambda — flexible signatures, anonymous functions

**Module 1.3 — Data-engineering Python**
- ✅ Generators & iterators — `yield`, lazy streams, memory efficiency
- ✅ Decorators — wrapping behaviour (timing, retry, logging)
- ✅ Dataclasses & OOP — `@dataclass`, classes, methods
- ✅ `collections` & `itertools` — `Counter`, `defaultdict`, `groupby`, `chain`
- 🔜 Type hints — `typing`, `Optional`, `list[...]`, why DE teams enforce them
- 🔜 Dates & times — `datetime`, parsing, timezones
- 🔜 Files & `pathlib` — context managers (`with`), reading/writing
- 🔜 JSON & CSV — `json`, `csv`, serialising records
- 🔜 Logging — structured logs over `print`
- 🔜 Testing — `assert`, designing for testability

---

## Track 2 — PySpark Foundations & DataFrames

> *Goal: think in distributed DataFrames; read, transform, and write any data.*
> Docs: <https://spark.apache.org/docs/latest/sql-getting-started.html>

**Module 2.1 — Getting started**
- ✅ Your first DataFrame — `createDataFrame`, rows & columns
- 🔜 SparkSession & architecture — driver/executors, lazy evaluation, the DAG
- 🔜 RDD basics — `map`/`filter`/`reduce`, when low-level still matters
- 🔜 Explicit schemas — `StructType`/`StructField`, DDL strings, `inferSchema`
- 🔜 Reading & writing files — CSV, JSON, **Parquet**, ORC; modes & options

**Module 2.2 — Core transformations**
- ✅ Select & filter — projection and row filtering
- ✅ Derive columns — `withColumn`, expressions
- 🔜 Conditionals — `when`/`otherwise`, `coalesce`
- 🔜 String functions — `concat`, `substring`, `upper`, `split`, `regexp_replace`
- 🔜 Date/time functions — `to_date`, `datediff`, `date_format`, `year`/`month`
- 🔜 Math & rounding — `round`, `abs`, casting numbers
- 🔜 Null handling — `na.fill`/`na.drop`, `isNull`, `coalesce`
- 🔜 Casting types — `cast`, schema enforcement
- 🔜 Sort, distinct & dedup — `orderBy`, `distinct`, `dropDuplicates`
- 🔜 Union & set ops — `unionByName`, `subtract`, `intersect`

**Module 2.3 — Aggregations & joins**
- ✅ Group & aggregate — `groupBy().agg()`, `sum`/`avg`/`count`/`collect_list`
- ✅ Joins — inner join on a key
- 🔜 All join types — left/right/outer/semi/anti, join conditions
- 🔜 Pivot — `groupBy().pivot()`
- ✅ Spark SQL — temp views, `spark.sql(...)`, the catalog
- ✅ Window functions — `row_number`, partitions, ranking
- 🔜 More windows — `rank`/`dense_rank`, `lag`/`lead`, running totals

**Module 2.4 — Complex & semi-structured data**
- 🔜 Arrays — `array`, `explode`, `array_contains`, `size`
- 🔜 Maps & structs — nested columns, dot access, `struct`
- 🔜 UDFs & pandas UDFs — custom logic, and why to prefer built-ins / vectorised UDFs

---

## Track 3 — Performance & Internals

> *Goal: understand what Spark does under the hood and make it fast.*
> Docs: <https://spark.apache.org/docs/latest/sql-performance-tuning.html> · <https://spark.apache.org/docs/latest/tuning.html>

- 🔜 Lazy evaluation & the DAG — transformations vs actions
- 🔜 Narrow vs wide transformations — what triggers a shuffle
- 🔜 Repartition vs coalesce — controlling partitions
- 🔜 Caching & persistence — `cache`/`persist`, storage levels
- 🔜 Broadcast joins — `broadcast()`, when small-table joins win
- 🔜 Catalyst & Adaptive Query Execution (AQE) — the optimiser, dynamic re-planning
- 🔜 Data skew & salting — diagnosing and fixing skewed keys
- 🔜 Bucketing & partition pruning — write-time layout, predicate pushdown
- 🔜 Reading the Spark UI — stages, tasks, shuffle read/write

---

## Track 4 — Structured Streaming

> *Goal: build real-time pipelines with the same DataFrame API.*
> Docs: <https://spark.apache.org/docs/latest/streaming/index.html>

**Module 4.1 — Streaming basics**
- ✅ Your first stream — `readStream`/`writeStream`, memory sink, `AvailableNow`
- 🔜 Sources & sinks — file, rate, socket, Kafka; console/memory/file/`foreachBatch`
- 🔜 Output modes — append, update, complete
- 🔜 Triggers — `processingTime`, `availableNow`, continuous

**Module 4.2 — Stateful streaming**
- ✅ Streaming aggregation — running counts, `complete` mode
- ✅ Event-time windows & watermarks — tumbling windows, late data
- 🔜 Sliding & session windows — overlapping and gap-based windows
- 🔜 Streaming deduplication — `dropDuplicates` with watermark
- 🔜 Stream-static joins — enriching a stream with reference data
- 🔜 Stream-stream joins — joining two live streams with watermarks
- 🔜 `foreachBatch` — custom sinks, upserts, writing to multiple targets
- 🔜 Checkpointing & fault tolerance — exactly-once, recovery

---

## Track 5 — Lakehouse & Delta Lake

> *Goal: bring ACID, upserts, and time travel to data lakes.*
> Docs: <https://docs.delta.io/latest/delta-intro.html>

- 🔜 Parquet deep-dive — columnar storage, predicate/column pushdown
- 🔜 Delta basics — ACID transactions, the transaction log
- 🔜 `MERGE` / upserts — change-data-capture into a table
- 🔜 Time travel — querying previous versions
- 🔜 Schema evolution & enforcement — safe column changes
- 🔜 Medallion architecture — bronze → silver → gold layers

---

## Track 6 — Capstone ETL Projects

> *Goal: combine everything into production-shaped pipelines.*

- 🔜 Batch ETL pipeline — ingest → clean → model → write Parquet
- 🔜 Data-quality checks — row counts, null/range assertions, quarantine
- 🔜 Incremental / CDC pattern — process only new data idempotently
- 🔜 Streaming pipeline capstone — end-to-end real-time aggregation with checkpointing

---

### Phased rollout

This is a living curriculum. Phase 1 ships the full syllabus above plus the first
expansion batch of graded lessons (Modules 1.2–1.3 and 2.x). Subsequent phases add
the remaining 🔜 lessons track by track until every topic is a runnable, graded
challenge. Track progress in the app — solved counts and badges update per track.
