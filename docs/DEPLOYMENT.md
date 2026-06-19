# Deployment & Publishing Guide

Exact, copy-paste steps to put SparkQuest on GitHub, deploy it live, and share it.
Replace `saipavan333` if your username differs. Commands assume you're in the
project root.

---

## 1. Publish to GitHub

### 1a. Initialize and first commit

```bash
git init
git add .
git commit -m "feat: SparkQuest — learn Python, PySpark & Spark Streaming by playing"
git branch -M main
```

### 1b. Create the public repo and push

**Option A — GitHub CLI (recommended):**

```bash
gh repo create saipavan333/sparkquest --public --source=. --remote=origin \
  --description "Gamified browser-based course to learn Python, PySpark & Spark Structured Streaming — real Spark execution, auto-grading, and an AI tutor." --push
```

**Option B — Web UI:** create an empty public repo named `sparkquest` at
github.com/new (no README/license), then:

```bash
git remote add origin https://github.com/saipavan333/sparkquest.git
git push -u origin main
```

### 1c. Create the public "AI projects" branch

You asked for a dedicated branch for your AI/ML projects. Note: for a single
portfolio project a **standalone public repo** (above) is what recruiters expect and
what the badges/CI assume. To *also* honor a dedicated branch:

```bash
git checkout -b ai-projects
git push -u origin ai-projects
git checkout main
```

Both `main` and `ai-projects` trigger CI and image publishing (see the workflows).
On GitHub, set the repo's default branch under **Settings → Branches** if you want
`ai-projects` to be the landing branch.

### 1d. Add the "About" description and topics (tags)

Repo home → ⚙️ (Edit, top-right of the About box):

- **Description:**
  `Gamified browser-based course to learn Python, PySpark & Spark Structured Streaming — real Spark execution, auto-grading, and an AI tutor.`
- **Website:** your Hugging Face Space URL (after step 2).
- **Topics:** `pyspark` `apache-spark` `spark-streaming` `data-engineering`
  `fastapi` `python` `docker` `edtech` `gamification` `ai-tutor` `mlops`
  `huggingface` `portfolio`

Or via CLI:

```bash
gh repo edit saipavan333/sparkquest \
  --add-topic pyspark --add-topic apache-spark --add-topic spark-streaming \
  --add-topic data-engineering --add-topic fastapi --add-topic python \
  --add-topic docker --add-topic edtech --add-topic gamification \
  --add-topic ai-tutor --add-topic mlops --add-topic huggingface --add-topic portfolio
```

GitHub Actions run automatically on push (badge turns green when CI passes).

---

## 2. Deploy the live demo — Hugging Face Space (free)

The fastest public, clickable demo. Uses your HF account; the Space runs the Docker
image.

### Scripted (recommended)

```bash
pip install "huggingface_hub>=0.23"
export HF_TOKEN=hf_xxx          # https://huggingface.co/settings/tokens (write)
export HF_USER=saipavan333
export HF_SPACE=sparkquest
bash scripts/deploy_hf.sh        # creates the Space (sdk=docker) and pushes
```

Your demo will be live at `https://huggingface.co/spaces/saipavan333/sparkquest`
(first build takes a few minutes).

### Automate it from CI

Add a repository secret **`HF_TOKEN`** (GitHub → Settings → Secrets and variables →
Actions → New repository secret). The `deploy-hf.yml` workflow then redeploys on
every push to `main`. (Without the secret it safely no-ops.)

---

## 3. Container image — GHCR (automatic) & Docker Hub (optional)

**GHCR:** `docker-publish.yml` builds and pushes to
`ghcr.io/saipavan333/sparkquest` on every push to `main`/`ai-projects` and on
version tags — no setup needed (uses the built-in `GITHUB_TOKEN`). After the first
run, make the package public: GitHub profile → Packages → `sparkquest` → Package
settings → Change visibility → Public. Then anyone can:

```bash
docker run -p 7860:7860 ghcr.io/saipavan333/sparkquest:latest
```

**Docker Hub (optional):** add secrets `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN`,
then add a `docker/login-action` step for `docker.io` and include
`docker.io/saipavan333/sparkquest` in the `images:` list of `docker-publish.yml`.

---

## 4. Alternative hosts

**Render** (Docker, `deploy/render.yaml`): New → Blueprint → connect the repo →
Render reads `render.yaml` and builds the Dockerfile. Health check `/healthz`.

**Fly.io** (`deploy/fly.toml`):

```bash
fly launch --copy-config --no-deploy   # reuse deploy/fly.toml
fly deploy
```

---

## 5. Environment variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `SPARKQUEST_PORT` | Port to bind | `7860` |
| `SPARKQUEST_ENV` | `development` / `production` | `development` |
| `SPARK_LOCAL_IP` | Pin Spark to loopback | `127.0.0.1` (set in image) |
| `SQ_TUTOR_PROVIDER` | `anthropic`/`openai`/`huggingface`/`none` | `none` |
| `SQ_TUTOR_MODEL`, `SQ_TUTOR_API_KEY` | LLM tutor config | — |
| `WANDB_API_KEY`, `WANDB_PROJECT` | Benchmark tracking | — |

---

## 6. Share on LinkedIn

Copy is ready in [LINKEDIN_POST.md](LINKEDIN_POST.md) — paste, attach a screenshot or
short screen recording of the live demo, and post.
