# Benchmarks

Reproducible PySpark performance measurements that back the figures and tables in
the [paper](../paper/). Every phase writes JSON to `results/`; the `figures` phase
renders PNGs into `../paper/figures/` and CSVs into `results/`.

## Run

```bash
pip install -r ../requirements-dev.txt
SPARK_LOCAL_IP=127.0.0.1 python run_benchmarks.py --all          # everything
# or individual, resumable phases:
python run_benchmarks.py --phase size
python run_benchmarks.py --phase parallelism --cores 1 2 4
python run_benchmarks.py --phase streaming
python run_benchmarks.py --phase grader
python run_benchmarks.py --phase figures
```

## Weights & Biases

Add `--wandb` to log every phase as a W&B run:

```bash
export WANDB_API_KEY=...           # or WANDB_MODE=offline for a local run
export WANDB_PROJECT=sparkquest-benchmarks
python run_benchmarks.py --all --wandb
```

## Reference environment

The committed `results/` were produced on: Ubuntu 22.04, Python 3.10, OpenJDK 11,
PySpark 3.5.1, **2 vCPU / 3.8 GiB RAM**. Numbers are hardware-dependent; re-run on
your machine to regenerate. The scaling study is intentionally run on a 2-core box,
which is why speed-up flattens past 2 workers (the small, overhead-bound job leaves
little to parallelise) — discussed in the paper's *Threats to Validity*.
