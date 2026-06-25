# 06 · Data Modeling for Analytics

> *Interview lens: "Model a schema for an e-commerce analytics warehouse." Then
> the inevitable "How do you track a customer who changes address?" — that's SCD
> Type 2, and getting it right is a senior signal.*

> **In plain words.** "Data modelling" just means deciding how to lay out your tables
> so they're easy and fast to report on. The classic pattern is a big **facts** table
> (the events — sales, clicks) surrounded by small **dimension** tables (the details —
> customers, products), called a *star schema*. This chapter covers that, plus how to
> keep **history** when details change over time (the "SCD Type 2" idea, which you
> also practise in [cap-04](../../lessons/capstone/cap-04-scd2.yaml)).

## 1. OLTP vs OLAP (why modeling differs)

- **OLTP** (transactional apps) — many small reads/writes, **normalized** (3NF) to
  avoid update anomalies and redundancy. Optimized for *writes/consistency*.
- **OLAP** (analytics/warehouse) — few huge reads, **denormalized** for query
  speed (fewer joins). Optimized for *reads*. This is your world as a DE.

Normalization removes redundancy (one fact in one place); analytics deliberately
*re-introduces* some redundancy (denormalization) so a dashboard query doesn't join
twelve tables.

## 2. Dimensional modeling (Kimball)

Split the world into **facts** and **dimensions**:

- **Fact table** — the *measurements* of a business process: numeric, mostly
  **additive** measures (`amount`, `quantity`), plus **foreign keys** to
  dimensions and a timestamp. Long and narrow; this is the big table. Example:
  `fact_sales(date_key, product_key, customer_key, store_key, quantity, amount)`.
- **Dimension table** — the *context* / descriptive attributes you filter and group
  by: `dim_customer(customer_key, name, segment, city, country)`,
  `dim_product`, `dim_date`. Short and wide.

**Grain** — the first thing to pin down: *what does one fact row mean?* ("one row
per order line item per day"). Everything else follows from the grain. State it
explicitly in interviews.

### Star vs snowflake
- **Star schema** — dimensions are **denormalized** (flat). Fast (one join per
  dimension), simple. The default.
- **Snowflake schema** — dimensions are **normalized** into sub-tables (e.g.
  product → category → department). Saves space, more joins, slower. Use sparingly.

### Measure additivity
- **Additive** — summable across all dimensions (revenue). 
- **Semi-additive** — summable across some, not time (account balance: you don't
  sum balances across days, you take the latest). 
- **Non-additive** — ratios/percentages (sum then recompute, don't sum the ratio).

### Fact table types
- **Transaction** — one row per event (most common).
- **Periodic snapshot** — state at regular intervals (daily inventory).
- **Accumulating snapshot** — one row per process instance, updated as it moves
  through milestones (order placed → shipped → delivered).

### Keys
Use **surrogate keys** (meaningless integers) for dimension PKs, not natural/business
keys. Why: business keys change, can be reused, and — crucially — surrogate keys let
you keep **multiple versions** of the same business entity (SCD2, next).

## 3. Slowly Changing Dimensions (SCD) — the money question

A customer moves city. How do you store it? Depends on whether you need **history**.

| Type | Behavior | History? |
|------|----------|----------|
| **0** | never change (e.g. date-of-birth) | n/a |
| **1** | **overwrite** the attribute in place | **no** — you lose the past |
| **2** | **add a new row** for the new version; keep the old | **full** |
| **3** | keep a "previous value" column | limited (one prior) |
| **4** | move history to a separate table | yes (split) |
| **6** | hybrid 1+2+3 | full + current-on-row |

**Type 2 is the one to know.** Each version is a row with:
`customer_key` (surrogate, unique per version), the business key, the attributes,
and metadata: `effective_date`, `end_date`, `is_current`. The current version has
`end_date = NULL`/`9999-12-31` and `is_current = true`. Facts join to the
surrogate key that was current *at the time of the event*, so history is preserved.

**Implementing SCD2 with Delta `MERGE`** (the practical follow-up):
1. For an incoming changed record matching a current row whose attributes differ:
   **close** the old version (`end_date = today`, `is_current = false`).
2. **Insert** a new current version (new surrogate key, `is_current = true`).
3. For a brand-new business key: just insert a current version.

Delta `MERGE` does steps in one atomic transaction (the `delta-02` lesson teaches
the MERGE mechanics; SCD2 layers this logic on top).

## 4. Beyond Kimball (one line each)

- **Inmon** — top-down, normalized enterprise warehouse feeding downstream marts.
- **Data Vault** — hubs/links/satellites; auditable, agile for changing sources;
  common in regulated enterprises.
- **One Big Table (OBT)/wide tables** — fully denormalized; cheap columnar storage
  makes "just make it wide" viable for many BI use cases today.

In a modern lakehouse you'll often land **bronze→silver** normalized-ish, then
build **gold** as Kimball star schemas or wide tables for serving.

---

**Next:** [07 · Pipeline System Design](07-system-design.md) — assembling all of
this into a pipeline you can whiteboard.
