"""Windows-friendly Hugging Face Space deploy — no bash, no rsync required.

Usage (PowerShell):
    pip install huggingface_hub
    $env:HF_TOKEN="hf_yourWriteToken"
    python scripts/deploy_hf.py --user <your-hf-username> --space sparkquest

Creates the Docker Space if needed, uploads the runtime files, and sets the
Space card (README with Hugging Face front matter). The build starts automatically.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    ap = argparse.ArgumentParser(description="Deploy SparkQuest to a Hugging Face Docker Space")
    ap.add_argument("--user", required=True, help="Your Hugging Face username")
    ap.add_argument("--space", default="sparkquest", help="Space name (default: sparkquest)")
    ap.add_argument("--token", default=os.environ.get("HF_TOKEN", ""), help="HF write token (or set HF_TOKEN)")
    args = ap.parse_args()

    if not args.token:
        sys.exit("No token. Set $env:HF_TOKEN to a WRITE token, or pass --token hf_...")

    try:
        from huggingface_hub import HfApi
    except ImportError:
        sys.exit("Missing dependency. Run:  pip install huggingface_hub")

    api = HfApi(token=args.token)
    repo_id = f"{args.user}/{args.space}"

    print(f"-> Ensuring Space {repo_id} exists (sdk=docker)...")
    api.create_repo(repo_id=repo_id, repo_type="space", space_sdk="docker", exist_ok=True)

    print("-> Uploading runtime files (app, lessons, data, Dockerfile, requirements)...")
    api.upload_folder(
        folder_path=str(ROOT),
        repo_id=repo_id,
        repo_type="space",
        allow_patterns=["app/**", "lessons/**", "data/**", "Dockerfile", "requirements.txt"],
        ignore_patterns=["**/__pycache__/**", "data/raw/**"],
        commit_message="Deploy SparkQuest",
    )

    print("-> Setting the Space card (README with HF front matter)...")
    api.upload_file(
        path_or_fileobj=str(ROOT / "deploy" / "huggingface" / "README.md"),
        path_in_repo="README.md",
        repo_id=repo_id,
        repo_type="space",
        commit_message="Set Space card",
    )

    print(f"\nDone! The Docker build starts automatically. Watch it here:\n  https://huggingface.co/spaces/{repo_id}")


if __name__ == "__main__":
    main()
