# Data

Small, finance-flavoured **sample** datasets bundled with the repo so every
lesson, test, and benchmark runs deterministically with zero external downloads.

| File | Used by | Description |
|------|---------|-------------|
| `sample/transactions.csv` | PySpark lessons, benchmarks | 10 synthetic equity trades (ticker, sector, side, price, qty, ts). |
| `sample/stream_events/*.json` | Streaming lessons | Newline-delimited JSON micro-batches (8 events) used as a file-based streaming source. |

## Scaling up with Kaggle (optional)

The benchmark suite can generate larger synthetic datasets on the fly
(`benchmarks/run_benchmarks.py --rows 5000000`). To instead use a real public
dataset, fetch one with the Kaggle API:

```bash
pip install kaggle
# Put your token at ~/.kaggle/kaggle.json (chmod 600), then:
bash scripts/fetch_kaggle_data.sh
```

See `scripts/fetch_kaggle_data.sh` for the exact dataset slug and post-processing.
The full Kaggle datasets are **not** committed — only the small sample above is.
