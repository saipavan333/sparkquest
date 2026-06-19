"""SparkQuest benchmark suite.

Measures real PySpark performance on the host machine and (optionally) logs every
run to Weights & Biases. Results are written as JSON to ``benchmarks/results/``
and turned into figures/tables for the paper via the ``figures`` phase.

Phases (resumable — re-running skips work already recorded):

    python benchmarks/run_benchmarks.py --phase size
    python benchmarks/run_benchmarks.py --phase parallelism --cores 1 2 4
    python benchmarks/run_benchmarks.py --phase streaming
    python benchmarks/run_benchmarks.py --phase grader
    python benchmarks/run_benchmarks.py --phase figures        # build PNGs + CSVs
    python benchmarks/run_benchmarks.py --all                  # everything

Add ``--wandb`` to log metrics to W&B (needs WANDB_API_KEY, or WANDB_MODE=offline).
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
RESULTS = ROOT / "benchmarks" / "results"
FIGURES = ROOT / "paper" / "figures"
RESULTS.mkdir(parents=True, exist_ok=True)


# ----------------------------- helpers -----------------------------

def _path(phase: str) -> Path:
    return RESULTS / f"{phase}.json"


def save(phase: str, data: dict) -> None:
    _path(phase).write_text(json.dumps(data, indent=2))


def load(phase: str) -> dict | None:
    p = _path(phase)
    return json.loads(p.read_text()) if p.exists() else None


def timed(fn, repeat: int) -> list[float]:
    times = []
    for _ in range(repeat):
        t = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t)
    return times


def wandb_log(enabled: bool, phase: str, config: dict, metrics_rows: list[dict]) -> None:
    if not enabled:
        return
    try:
        import wandb
    except ImportError:
        print("  [wandb] not installed; skipping")
        return
    run = wandb.init(
        project=os.getenv("WANDB_PROJECT", "sparkquest-benchmarks"),
        entity=os.getenv("WANDB_ENTITY") or None,
        name=f"{phase}",
        config=config,
        reinit=True,
    )
    for row in metrics_rows:
        run.log(row)
    run.finish()
    print(f"  [wandb] logged {len(metrics_rows)} rows for phase '{phase}'")


# ----------------------------- workload -----------------------------

def agg_workload(spark, n: int) -> int:
    """A representative shuffle-heavy job: group-by aggregation over n rows."""
    from pyspark.sql import functions as F

    df = spark.range(n).select(
        (F.col("id") % 1000).alias("key"),
        (F.rand(seed=42) * 100).alias("val"),
    )
    res = df.groupBy("key").agg(
        F.sum("val").alias("s"), F.avg("val").alias("a"), F.count(F.lit(1)).alias("c")
    )
    return res.count()  # force execution


# ----------------------------- phases -----------------------------

def phase_size(args) -> None:
    from app.core.spark_session import build_spark_session

    existing = load("size") or {"master": args.master, "repeat": args.repeat, "runs": []}
    done = {r["rows"] for r in existing["runs"]}
    todo = [n for n in args.sizes if n not in done]
    if not todo:
        print("size: nothing to do")
        return

    spark = build_spark_session(master=args.master, app_name="bench-size")
    agg_workload(spark, 50_000)  # warm up the JVM / caches
    for n in todo:
        ts = timed(lambda n=n: agg_workload(spark, n), args.repeat)
        med = statistics.median(ts)
        rec = {"rows": n, "median_s": med, "throughput_rows_per_s": n / med, "times": ts}
        existing["runs"].append(rec)
        existing["runs"].sort(key=lambda r: r["rows"])
        save("size", existing)
        print(f"  size {n:>9,}: {med:7.3f}s  ->  {n / med:>12,.0f} rows/s")
    spark.stop()
    wandb_log(args.wandb, "size", {"master": args.master},
              [{"rows": r["rows"], "throughput_rows_per_s": r["throughput_rows_per_s"],
                "median_s": r["median_s"]} for r in existing["runs"]])


def phase_parallelism(args) -> None:
    from app.core.spark_session import build_spark_session

    existing = load("parallelism") or {"rows": args.par_rows, "repeat": args.repeat, "runs": []}
    done = {r["cores"] for r in existing["runs"]}
    for k in args.cores:
        if k in done:
            continue
        spark = build_spark_session(master=f"local[{k}]", app_name=f"bench-par-{k}")
        agg_workload(spark, 50_000)
        ts = timed(lambda s=spark: agg_workload(s, args.par_rows), args.repeat)
        med = statistics.median(ts)
        existing["runs"].append({"cores": k, "median_s": med})
        existing["runs"].sort(key=lambda r: r["cores"])
        save("parallelism", existing)
        print(f"  cores {k}: {med:7.3f}s")
        spark.stop()
    if existing["runs"]:
        base = existing["runs"][0]["median_s"]
        for r in existing["runs"]:
            r["speedup"] = round(base / r["median_s"], 3)
        save("parallelism", existing)
    wandb_log(args.wandb, "parallelism", {"rows": args.par_rows},
              [{"cores": r["cores"], "median_s": r["median_s"], "speedup": r.get("speedup")}
               for r in existing["runs"]])


def phase_streaming(args) -> None:
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    from app.core.spark_session import build_spark_session

    d = ROOT / "data" / "raw" / "stream_bench"
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)
    tickers = ["AAPL", "GOOG", "MSFT", "JPM", "XOM"]
    total = 0
    for i in range(args.files):
        with open(d / f"part-{i:03d}.json", "w") as fh:
            for _ in range(args.per_file):
                fh.write(json.dumps({"ticker": random.choice(tickers),
                                     "amount": random.randint(10, 1000)}) + "\n")
                total += 1

    schema = StructType([StructField("ticker", StringType()), StructField("amount", LongType())])
    spark = build_spark_session(master=args.master, app_name="bench-stream")
    t = time.perf_counter()
    q = (
        spark.readStream.schema(schema).json(str(d))
        .groupBy("ticker").count()
        .writeStream.format("memory").queryName("bench_out")
        .outputMode("complete").trigger(availableNow=True).start()
    )
    q.awaitTermination()
    elapsed = time.perf_counter() - t
    groups = spark.table("bench_out").count()
    spark.stop()
    data = {"files": args.files, "events": total, "elapsed_s": elapsed,
            "throughput_events_per_s": total / elapsed, "groups": groups}
    save("streaming", data)
    print(f"  streaming: {total:,} events across {args.files} files in {elapsed:.2f}s "
          f"-> {total / elapsed:,.0f} events/s")
    wandb_log(args.wandb, "streaming", {"files": args.files, "per_file": args.per_file},
              [{"throughput_events_per_s": total / elapsed, "events": total, "elapsed_s": elapsed}])


def phase_grader(args) -> None:
    from app.core.executor import run_job

    py = []
    for _ in range(args.repeat + 2):
        r = run_job("x = sum(range(1000))",
                    checks=[{"type": "var_equals", "name": "x", "expected": 499500, "message": "m"}],
                    timeout=15)
        py.append(r.duration_ms)
    sp_total, sp_start = [], []
    for _ in range(3):
        r = run_job("df = spark.range(10)\nn = df.count()", needs_spark=True,
                    checks=[{"type": "custom", "code": "ok = True", "message": "m"}], timeout=60)
        sp_total.append(r.duration_ms)
        sp_start.append(r.spark_startup_ms)
    data = {
        "python_ms": py, "python_median_ms": statistics.median(py),
        "spark_total_ms": sp_total, "spark_total_median_ms": statistics.median(sp_total),
        "spark_startup_ms": sp_start, "spark_startup_median_ms": statistics.median(sp_start),
    }
    save("grader", data)
    print(f"  grader: python {data['python_median_ms']}ms | "
          f"spark startup {data['spark_startup_median_ms']}ms | "
          f"spark total {data['spark_total_median_ms']}ms")
    wandb_log(args.wandb, "grader", {},
              [{"python_median_ms": data["python_median_ms"],
                "spark_startup_median_ms": data["spark_startup_median_ms"],
                "spark_total_median_ms": data["spark_total_median_ms"]}])


def phase_figures(args) -> None:
    import csv

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    FIGURES.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"figure.dpi": 130, "font.size": 11, "axes.grid": True,
                         "grid.alpha": 0.3, "savefig.bbox": "tight"})
    ORANGE = "#ff7a18"

    size = load("size")
    if size and size["runs"]:
        rows = [r["rows"] for r in size["runs"]]
        thr = [r["throughput_rows_per_s"] for r in size["runs"]]
        fig, ax = plt.subplots(figsize=(5, 3.2))
        ax.plot([r / 1e6 for r in rows], [t / 1e6 for t in thr], "o-", color=ORANGE, lw=2)
        ax.set_xlabel("Dataset size (million rows)")
        ax.set_ylabel("Throughput (M rows/s)")
        ax.set_title("Aggregation throughput vs dataset size")
        fig.savefig(FIGURES / "throughput.png")
        plt.close(fig)

    par = load("parallelism")
    if par and par["runs"]:
        cores = [r["cores"] for r in par["runs"]]
        speed = [r.get("speedup", 1.0) for r in par["runs"]]
        fig, ax = plt.subplots(figsize=(5, 3.2))
        ax.plot(cores, cores, "--", color="#888", label="ideal linear")
        ax.plot(cores, speed, "o-", color=ORANGE, lw=2, label="measured")
        ax.set_xlabel("Spark cores (local[k])")
        ax.set_ylabel("Speed-up vs 1 core")
        ax.set_title("Strong-scaling speed-up")
        ax.legend()
        fig.savefig(FIGURES / "speedup.png")
        plt.close(fig)

    grader = load("grader")
    if grader:
        labels = ["Python\nchallenge", "Spark\nstartup", "Spark\nchallenge (total)"]
        vals = [grader["python_median_ms"], grader["spark_startup_median_ms"],
                grader["spark_total_median_ms"]]
        fig, ax = plt.subplots(figsize=(5, 3.2))
        ax.bar(labels, vals, color=[ORANGE, "#4aa8ff", "#3fb950"])
        ax.set_ylabel("Median latency (ms)")
        ax.set_title("Auto-grader latency")
        for i, v in enumerate(vals):
            ax.text(i, v, f"{int(v)}", ha="center", va="bottom", fontsize=9)
        fig.savefig(FIGURES / "grader_latency.png")
        plt.close(fig)

    # CSV summaries for the paper tables
    if size and size["runs"]:
        with open(RESULTS / "size.csv", "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["rows", "median_s", "throughput_rows_per_s"])
            for r in size["runs"]:
                w.writerow([r["rows"], f"{r['median_s']:.3f}", f"{r['throughput_rows_per_s']:.0f}"])
    if par and par["runs"]:
        with open(RESULTS / "parallelism.csv", "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["cores", "median_s", "speedup"])
            for r in par["runs"]:
                w.writerow([r["cores"], f"{r['median_s']:.3f}", r.get("speedup")])
    print(f"  figures + CSVs written to {FIGURES} and {RESULTS}")


PHASES = {
    "size": phase_size,
    "parallelism": phase_parallelism,
    "streaming": phase_streaming,
    "grader": phase_grader,
    "figures": phase_figures,
}


def main() -> None:
    ap = argparse.ArgumentParser(description="SparkQuest benchmark suite")
    ap.add_argument("--phase", choices=list(PHASES))
    ap.add_argument("--all", action="store_true", help="run every phase in order")
    ap.add_argument("--master", default="local[2]")
    ap.add_argument("--sizes", type=int, nargs="+", default=[100_000, 500_000, 1_000_000, 2_000_000])
    ap.add_argument("--cores", type=int, nargs="+", default=[1, 2, 4])
    ap.add_argument("--par-rows", type=int, default=1_000_000)
    ap.add_argument("--files", type=int, default=40)
    ap.add_argument("--per-file", type=int, default=2_500)
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--wandb", action="store_true", help="log metrics to Weights & Biases")
    args = ap.parse_args()

    order = ["size", "parallelism", "streaming", "grader", "figures"]
    todo = order if args.all else ([args.phase] if args.phase else [])
    if not todo:
        ap.error("pass --phase <name> or --all")
    for phase in todo:
        print(f"== phase: {phase} ==")
        PHASES[phase](args)


if __name__ == "__main__":
    main()
