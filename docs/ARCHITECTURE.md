# Architecture

SparkQuest is a single-container web application: a FastAPI backend that serves a
static single-page frontend and a small JSON API, executing learner code against a
real Spark engine in an isolated child process.

## Components

| Layer | Tech | Responsibility |
|-------|------|----------------|
| Frontend | Vanilla JS + Monaco editor | Lesson UI, code editor, console, tutor panel, gamification |
| API | FastAPI + Pydantic | Typed endpoints, request/response validation |
| Catalog | YAML + Pydantic | Loads lessons; exposes a *public* view that hides solutions & grader checks |
| Executor | `subprocess` | Spawns an isolated child per submission; enforces timeout & output caps |
| Harness | PySpark | Runs code, injects a tuned `SparkSession`, evaluates checks, emits a JSON verdict |
| Grader | Python | Pass/fail decision + difficulty-scaled XP |
| Gamification | In-memory store | XP, levels, badges, leaderboard |
| Tutor | httpx | Pluggable LLM provider with offline rule-based fallback |

## Request flow

1. The browser sends `POST /api/submit` with `{challenge_id, code, user_id}`.
2. The API looks up the challenge (including its hidden grader checks).
3. The **executor** writes a job spec to a temp file and spawns
   `python harness.py job.json result.json` with a hard timeout.
4. The **harness** (in the child process) optionally builds a `SparkSession`,
   executes the learner's code in an isolated namespace while capturing
   stdout/stderr, evaluates each declarative check, and writes a JSON verdict.
5. The **grader** reads the verdict, decides pass/fail (clean run **and** all checks
   pass), and awards XP; the gamification store updates levels/badges.
6. The API returns the verdict; the frontend renders checks, XP, and any new badges.

## Execution sandbox & security model

The current model is **process isolation + wall-clock timeout + output truncation**.
The API process never executes learner code; it only reads a result file. A hang,
crash, or OOM in a submission cannot take down the server.

This is appropriate for a **trusted, single-tenant educational demo**. It is *not* a
hostile-multi-tenant sandbox — submitted Python can import modules and touch the
local filesystem within the child process. Production hardening path:

- Run each submission in an **ephemeral, network-isolated container** (gVisor or
  nsjail), with read-only FS and dropped capabilities.
- Apply cgroup CPU/memory limits per submission.
- Keep the existing timeout/output caps as a second line of defense.

`SPARK_LOCAL_IP=127.0.0.1` is set so Spark never depends on resolving the machine
hostname (a frequent failure in containers/sandboxes).

## Auto-grading framework

Challenges declare typed checks evaluated against the post-run namespace:

- `stdout_contains`, `stdout_equals`
- `var_equals`, `var_close` (float tolerance), `var_type`
- `callable_returns`
- `df_columns`, `df_row_count`, `df_equals` (DataFrame compared as a **multiset** of
  rows — order-insensitive, matching Spark's non-deterministic ordering)
- `custom` (author-written assertion code, used for stateful streaming results)

Streaming lessons grade deterministically by using a bundled file source plus
`Trigger.AvailableNow`, which drains all input and stops, leaving an in-memory table
to assert against.

## Persistence

Progress lives in a thread-safe in-memory store whose interface (`get`,
`record_solve`, `leaderboard`) mirrors what a persistent backend would expose.
Swapping in Redis or Postgres is a localized change in `app/core/gamification.py`;
no API or frontend changes are required.

## Scaling

The dominant interactive cost is the per-submission Spark **cold start** (~3.5 s).
The intended production design replaces the per-submission `local[k]` session with a
thin **Spark Connect** client (`SparkSession.builder.remote("sc://…")`) talking to a
warm, shared Spark cluster, removing JVM startup from every interaction. The
`build_spark_session` factory is the single seam where this swap happens.
