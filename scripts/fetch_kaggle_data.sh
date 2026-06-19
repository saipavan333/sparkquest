#!/usr/bin/env bash
# Optional: fetch a larger, real dataset from Kaggle for benchmarking / extended
# lessons. The committed repo only ships the tiny deterministic sample in data/.
#
#   1) pip install kaggle
#   2) Put your token at ~/.kaggle/kaggle.json (chmod 600), or set KAGGLE_USERNAME/KAGGLE_KEY
#   3) bash scripts/fetch_kaggle_data.sh
set -euo pipefail

DATASET="${KAGGLE_DATASET:-camnugent/sandp500}"   # S&P 500 daily prices (finance-flavoured)
DEST="data/raw"

mkdir -p "$DEST"
echo "▶ Downloading ${DATASET} → ${DEST}"
kaggle datasets download -d "${DATASET}" -p "${DEST}" --unzip

echo "✅ Done. Point benchmarks at it, e.g.:"
echo "   python benchmarks/run_benchmarks.py --csv ${DEST}/all_stocks_5yr.csv"
