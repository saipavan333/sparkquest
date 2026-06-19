# syntax=docker/dockerfile:1
# SparkQuest — single-stage image. Mirrors the validated runtime:
# Python 3.10 + OpenJDK 11 + PySpark 3.5.1, served by uvicorn on port 7860.
FROM python:3.10-slim-bullseye

# --- System dependencies ---
# openjdk-11: Spark's JVM | procps: required by Spark launch scripts | curl: healthcheck
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-11-jre-headless \
        procps \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    # Pin Spark to loopback (no dependency on resolving the container hostname)
    SPARK_LOCAL_IP=127.0.0.1 \
    SPARKQUEST_ENV=production \
    SPARKQUEST_HOST=0.0.0.0 \
    SPARKQUEST_PORT=7860

# --- Non-root user (Hugging Face Spaces runs containers as uid 1000) ---
RUN useradd --create-home --uid 1000 user
WORKDIR /home/user/app
ENV HOME=/home/user PYTHONPATH=/home/user/app

# --- Python dependencies (cached layer) ---
COPY --chown=user:user requirements.txt .
RUN pip install -r requirements.txt

# --- Application code & content ---
COPY --chown=user:user app ./app
COPY --chown=user:user lessons ./lessons
COPY --chown=user:user data ./data

USER user
EXPOSE 7860

HEALTHCHECK --interval=30s --timeout=5s --start-period=45s --retries=3 \
    CMD curl -fsS http://localhost:7860/healthz || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "7860"]
