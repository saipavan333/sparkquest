.PHONY: help install dev run test test-fast lint fmt validate bench docker-build docker-run clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install:  ## Install runtime + dev dependencies
	pip install -r requirements-dev.txt

run:  ## Run the app locally (http://localhost:7860)
	SPARK_LOCAL_IP=127.0.0.1 python -m app.main

test:  ## Run the full test suite (includes slow Spark tests)
	SPARK_LOCAL_IP=127.0.0.1 pytest

test-fast:  ## Run only fast tests (no Spark startup)
	SPARK_LOCAL_IP=127.0.0.1 pytest -m "not spark"

validate:  ## Check every lesson's reference solution passes its grader
	SPARK_LOCAL_IP=127.0.0.1 python scripts/validate_lessons.py

lint:  ## Lint with ruff
	ruff check app tests scripts benchmarks

fmt:  ## Auto-format with ruff
	ruff format app tests scripts benchmarks

bench:  ## Run the Spark benchmark suite (writes benchmarks/results/)
	SPARK_LOCAL_IP=127.0.0.1 python benchmarks/run_benchmarks.py

docker-build:  ## Build the Docker image
	docker build -t sparkquest:local .

docker-run:  ## Run the Docker image
	docker run --rm -p 7860:7860 sparkquest:local

clean:  ## Remove caches and Spark scratch
	rm -rf .pytest_cache .ruff_cache __pycache__ */__pycache__ spark-warehouse metastore_db derby.log
