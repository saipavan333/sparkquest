#!/usr/bin/env bash
# Deploy SparkQuest to a Hugging Face *Docker* Space.
#
#   export HF_TOKEN=hf_xxx           # write token: https://huggingface.co/settings/tokens
#   export HF_USER=saipavan333       # your HF username (default below)
#   export HF_SPACE=sparkquest       # target space name
#   bash scripts/deploy_hf.sh
#
# Creates the Space if it doesn't exist, then syncs the runtime files and pushes.
set -euo pipefail

HF_USER="${HF_USER:-saipavan333}"
HF_SPACE="${HF_SPACE:-sparkquest}"
: "${HF_TOKEN:?Set HF_TOKEN to a Hugging Face write token}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "▶ Ensuring Space ${HF_USER}/${HF_SPACE} exists (sdk=docker)…"
python - <<PY
from huggingface_hub import HfApi
HfApi(token="${HF_TOKEN}").create_repo(
    repo_id="${HF_USER}/${HF_SPACE}",
    repo_type="space", space_sdk="docker", exist_ok=True,
)
print("  ok")
PY

echo "▶ Cloning Space repo…"
git clone --quiet "https://${HF_USER}:${HF_TOKEN}@huggingface.co/spaces/${HF_USER}/${HF_SPACE}" "$WORK/space"

echo "▶ Syncing runtime files…"
rsync -a --delete --exclude '.git' \
  "$ROOT/app" "$ROOT/lessons" "$ROOT/data" \
  "$ROOT/requirements.txt" "$ROOT/Dockerfile" \
  "$WORK/space/"
# The Space card (README with HF front matter) must be the repo README on HF.
cp "$ROOT/deploy/huggingface/README.md" "$WORK/space/README.md"

cd "$WORK/space"
git add -A
git config user.email "ci@sparkquest.local"
git config user.name "sparkquest-deploy"
git commit -m "Deploy SparkQuest $(date -u +%FT%TZ)" --quiet || { echo "No changes to deploy."; exit 0; }
git push --quiet

echo "✅ Deployed → https://huggingface.co/spaces/${HF_USER}/${HF_SPACE}"
