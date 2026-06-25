# 08 · Python & SQL Mastery for Data Engineers

> *Interview lens: PySpark is Python, and every pipeline is glued with SQL.
> Expect a Python concurrency question ("threads vs processes — why?") and a
> hard SQL one (window functions, gaps-and-islands). These are easy points if you
> drill them.*

> **In plain words.** Two skills sit under everything else: the **Python** you write
> Spark with, and the **SQL** you query data with. This chapter goes a little deeper
> into both — the Python features that come up a lot (and one famous quirk about
> running things at the same time), and the SQL patterns interviewers love, like
> window functions. If a term feels advanced, the matching lessons in the Python and
> PySpark tracks cover the same idea hands-on.

---

## Part A — Python deep

### A1. The GIL and the three concurrency models
The **Global Interpreter Lock** allows only **one thread to execute Python
bytecode at a time** (CPython). Consequence:

- **`threading`** — multiple threads, but the GIL serializes Python execution. They
  help only **I/O-bound** work, because a thread *releases the GIL while waiting on
  I/O* (network, disk). Great for "call 100 APIs / DB queries concurrently."
- **`multiprocessing`** — separate **processes**, each its own interpreter and GIL →
  **true parallelism** for **CPU-bound** work (heavy computation). Cost: process
  spawn + serialization (pickling) to pass data.
- **`asyncio`** — **single-threaded cooperative** concurrency via an event loop and
  `async`/`await`. Best for **massive I/O fan-out** (thousands of sockets) with low
  overhead, but your libraries must be async-aware.

DE ingestion (API/DB/file calls) is usually **I/O-bound** ⇒ threads or asyncio. Use
multiprocessing only for CPU-heavy local transforms (and remember Spark already
gives you distributed parallelism — don't reinvent it).

### A2. Generators & iterators (memory)
A **generator** (`yield`) produces values lazily, holding one at a time → constant
memory over huge or infinite sequences. An **iterator** implements `__iter__`/
`__next__`. This *is* the Spark philosophy in miniature: stream, don't materialize.
`itertools` (`chain`, `islice`, `groupby`, `tee`) composes them efficiently.

### A3. Decorators & context managers
- **Decorator** — a callable that wraps a function to add behavior (retry,
  timing, caching, auth) without editing it. `functools.wraps` preserves metadata;
  `functools.lru_cache` is a built-in memoizing decorator.
- **Context manager** — `with` + `__enter__`/`__exit__` (or `@contextmanager`)
  guarantees setup/teardown (close files, release locks, commit/rollback) **even on
  exception**. Every resource a pipeline touches should be in a `with`.

### A4. Typing
Type hints (`list[dict[str, int]]`, `Optional`, `TypedDict`, `dataclass`) don't run
but enable **static checking** (mypy/pyright), better IDE help, and self-
documenting interfaces. Mature DE teams enforce them in CI because a pipeline that
type-checks fails *before* it runs on 10 TB.

### A5. Testing data code
- **Pure functions** — keep transformations as small functions taking and returning
  DataFrames/values, so you can unit-test them on tiny fixtures with known expected
  output (exactly what SparkQuest's auto-grader does).
- **pytest** — fixtures for a shared `SparkSession`, parametrize over cases, mark
  slow/integration tests.
- **Assertions on data** — schema equality, row-level equality (order-insensitive),
  and data-quality checks (counts/nulls/ranges) as runtime gates.
- **Mocking** — isolate external systems (APIs, warehouses) so tests are fast and
  deterministic.

### A6. Memory & gotchas
Python variables are **references**; `b = a` aliases (mutating `b` mutates `a` for
mutable types). Default mutable arguments (`def f(x=[])`) are a classic bug (the
list persists across calls). `__slots__` cuts per-instance memory for many small
objects. Know that `toPandas()`/`collect()` pull *everything* to the driver.

---

## Part B — SQL deep

### B1. Window functions (the #1 SQL interview topic)
`func() OVER (PARTITION BY ... ORDER BY ... frame)` computes across a window of
rows **without collapsing them** (unlike `GROUP BY`).

- **Ranking** — `ROW_NUMBER()` (unique 1..n), `RANK()` (gaps on ties),
  `DENSE_RANK()` (no gaps), `NTILE(n)` (buckets).
- **Offset** — `LAG(col, k)` / `LEAD(col, k)` for previous/next rows (day-over-day
  change, deltas).
- **Running aggregates** — `SUM()/AVG() OVER (ORDER BY t ROWS BETWEEN UNBOUNDED
  PRECEDING AND CURRENT ROW)` for cumulative totals; `RANGE` vs `ROWS` frames matter
  (RANGE is value-based, ROWS is row-count-based).

### B2. CTEs & recursion
`WITH t AS (...)` names a subquery → readable, reusable, the building block of
complex analytics. **Recursive CTEs** (`WITH RECURSIVE`) walk hierarchies
(org charts, graph paths, bill-of-materials).

### B3. Advanced aggregation
`GROUP BY` + `HAVING` (filter *after* aggregation). `GROUPING SETS` / `ROLLUP` /
`CUBE` compute multiple aggregation levels (subtotals/grand totals) in one pass —
great for OLAP cubes.

### B4. The classic interview patterns
- **Nth-highest / top-N per group** — `ROW_NUMBER()`/`DENSE_RANK()` in a subquery,
  filter `rk = N` or `rk <= N`.
- **Gaps-and-islands** (sessionization, consecutive streaks) — difference of two
  row-number sequences, or `LAG` + cumulative sum of "new group" flags.
- **Deduplicate keep-latest** — `ROW_NUMBER() OVER (PARTITION BY key ORDER BY
  updated_at DESC)`, keep `= 1`.
- **Running total / moving average** — windowed `SUM`/`AVG` with a frame.
- **Pivot / conditional aggregation** — `SUM(CASE WHEN ... THEN ... END)`.
- **Self-join** — compare rows in a table to other rows (e.g. employee→manager,
  pairs within a group).
- **Find duplicates** — `GROUP BY cols HAVING COUNT(*) > 1`.

These map 1:1 to the PySpark patterns in the [question bank](interview-questions.md)
§D — the DataFrame API and SQL are two views of the same Catalyst plan.

### B5. Query optimization mindset
- **OLTP** — indexes (B-tree) make point lookups O(log n); the optimizer uses them.
- **Warehouse/lakehouse** — there are no traditional indexes; speed comes from
  **partition pruning**, **predicate/column pushdown**, **file layout** (Z-order),
  **broadcast joins**, and **good statistics** (see chapters 02/04). Avoid `SELECT *`
  (kills column pruning), filter early, and **read the plan** (`EXPLAIN`) to confirm
  pushdown happened and the join strategy is sane.

---

You've now got the full senior map: execution → performance → streaming → storage →
lakehouse → modeling → system design → language mastery. Pair every chapter with its
hands-on lessons, drill the [question bank](interview-questions.md), and follow the
6-week plan in [resources](resources.md). Go get the offer.
