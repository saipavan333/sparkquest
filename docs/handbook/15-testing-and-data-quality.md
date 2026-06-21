# 15 · Testing & Data Quality

> *Interview lens: "How do you test a Spark pipeline, and how do you stop bad data
> reaching production tables?" Senior data engineers are judged on **trust** —
> silent data corruption is worse than a loud crash, because no one notices until
> a dashboard is wrong for a month.*
>
> Prereqs: [07 · System Design](07-system-design.md). Grounded in
> [PyDeequ](https://pydeequ.readthedocs.io/),
> [Great Expectations](https://docs.greatexpectations.io/docs/), and
> [Delta constraints](https://docs.delta.io/latest/delta-constraints.html).

## 1. Why data pipelines need their own testing discipline

Application bugs throw exceptions; **data bugs return wrong answers silently**. A
join that quietly fans out, a timezone parsed wrong, a source that starts sending
`NULL`s — the job is "green" while the numbers rot. So you test **two things**:
the **code** (does the transform do what I think?) and the **data** (does today's
data look like valid data?).

## 2. The data testing pyramid

- **Unit tests** (many, fast) — pure **transform functions** of DataFrame → DataFrame,
  asserted on tiny hand-built inputs. No cluster, milliseconds.
- **Integration tests** (some) — wire several stages together against sample data,
  check the end-to-end output and schema.
- **Data quality checks** (in production, every run) — assertions on the *actual*
  data flowing through, because you can't unit-test data you haven't seen.

## 3. Unit-testing Spark transformations

Structure code so business logic is **pure functions** of DataFrames — easy to
test, no I/O:

```python
def enrich(df):                      # pure: DataFrame -> DataFrame
    return df.withColumn("amount_usd", F.col("amount") * F.col("fx_rate"))

# test (pytest + a session-scoped local SparkSession fixture)
def test_enrich(spark):
    inp = spark.createDataFrame([(10.0, 1.1)], ["amount", "fx_rate"])
    out = enrich(inp).collect()[0]
    assert out["amount_usd"] == 11.0
```

- **`chispa`** gives `assert_df_equality(actual, expected)` with nice diffs
  (schema + row-order-insensitive). It's the de-facto PySpark unit-test helper.
- Use a **session-scoped** `SparkSession` fixture (`local[*]`, UI off) so the JVM
  starts once for the whole suite — exactly how SparkQuest's own grader runs a
  tiny Spark per submission and compares DataFrames with a multiset equality.

## 4. The six data-quality dimensions (name them)

| Dimension | Question | Example check |
|---|---|---|
| **Completeness** | are required fields present? | `NULL` rate of `customer_id` = 0 |
| **Uniqueness** | any duplicates? | `count(*) == count(distinct id)` |
| **Validity** | values in the allowed domain? | `status IN ('NEW','PAID')`, `amount >= 0` |
| **Consistency** | do related values agree? | `total == sum(line_items)` |
| **Timeliness** | is data fresh / on time? | `max(event_ts)` within SLA |
| **Accuracy** | does it match reality/reference? | row count vs source system |

## 5. Assertion & enforcement patterns

```python
# fail fast on a broken invariant
bad = df.filter(F.col("amount") < 0).count()
assert bad == 0, f"{bad} rows have negative amount"

# schema enforcement — reject on read, don't infer in prod
df = spark.read.schema(expected_schema).json(path)
```

- **Schema enforcement** — declare the schema, don't `inferSchema` in production
  (drift silently changes types). Delta/Iceberg enforce schema on write.
- **Row-count deltas** — alert if today's volume swings beyond a band vs history.
- **Referential integrity** — anti-join against the dimension to find orphan keys.

## 6. The quarantine (dead-letter) pattern — don't drop silently

The senior move: **split** valid and invalid rows, write the bad ones to a
**quarantine** table with the reason, and keep the pipeline running:

```python
checked = df.withColumn("dq_error",
    F.when(F.col("amount") < 0, "negative_amount")
     .when(F.col("customer_id").isNull(), "missing_customer")
     .otherwise(F.lit(None)))

valid     = checked.filter(F.col("dq_error").isNull()).drop("dq_error")
quarantine = checked.filter(F.col("dq_error").isNotNull())
# write `valid` to the gold table, `quarantine` to a dead-letter table for review
```

This is exactly what lesson **sp-33** and capstone **cap-02** drill: never
`dropna()` and pretend the rows didn't exist — you'll be asked where the missing
revenue went.

## 7. Frameworks (one line each)

- **Great Expectations** — declarative "expectations" (e.g. `expect_column_values_to_not_be_null`)
  with data docs and validation results; popular for batch.
- **Deequ / PyDeequ** — Amazon's Spark-native DQ: `Check`s, **constraint
  suggestion**, and **anomaly detection** on metrics over time.
- **dbt tests** — `not_null`, `unique`, `accepted_values`, `relationships` in the
  warehouse/transform layer.
- **Delta constraints** — `NOT NULL` and **`CHECK`** constraints enforced on write
  so bad rows can't even land:

```sql
ALTER TABLE trades ADD CONSTRAINT positive_qty CHECK (qty > 0);
```

## 8. Testing streaming

Streaming is testable too: use the **memory sink** + **`Trigger.AvailableNow`** to
make a stream finite and deterministic, then assert on the resulting table —
precisely how SparkQuest grades its streaming lessons. For stateful logic, test
the `foreachBatch` function as a plain DataFrame transform.

## 9. Rapid-fire interview Q&A

- **How do you unit-test Spark code?** Pure DataFrame→DataFrame functions,
  `chispa.assert_df_equality` on tiny inputs, session-scoped local Spark fixture.
- **Six DQ dimensions?** Completeness, uniqueness, validity, consistency,
  timeliness, accuracy.
- **A required field starts arriving null — what happens in your pipeline?** A DQ
  check routes those rows to a **quarantine** table with a reason; the good rows
  proceed; an alert fires. Nothing is dropped silently.
- **Where do you enforce schema?** On read (`.schema(...)`, no `inferSchema`) and
  on write (Delta/Iceberg schema enforcement + `CHECK` constraints).
- **DQ frameworks you'd reach for?** Great Expectations or Deequ for batch; dbt
  tests in the warehouse; Delta `CHECK` constraints at the storage layer.
- **How do you make a streaming test deterministic?** Memory sink +
  `Trigger.AvailableNow`, then assert on the output table.

---

**Next:** [07 · Pipeline System Design](07-system-design.md) ties testing, DQ, and
idempotency into a production-shaped pipeline.
