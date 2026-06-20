"""Factory for tuned, low-overhead local SparkSessions.

Used by both the execution harness (per-submission sessions) and the benchmark
suite, so configuration stays consistent. In production this is the seam where
you would swap a fresh ``local[*]`` session for a thin Spark Connect client
(``.remote("sc://spark-server:15002")``) talking to a warm, shared cluster.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

# Delta Lake (Scala 2.12, matching PySpark 3.5). The JAR is fetched from Maven on
# first use; the Docker image pre-caches it so the live app needs no network.
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"

# Defaults tuned for fast startup and a small memory footprint, suitable for
# interactive learning and CI. Override per call via ``extra``.
_DEFAULT_CONF = {
    "spark.ui.enabled": "false",
    "spark.ui.showConsoleProgress": "false",
    "spark.sql.shuffle.partitions": "4",
    "spark.default.parallelism": "4",
    "spark.sql.adaptive.enabled": "true",
    "spark.driver.bindAddress": "127.0.0.1",
    "spark.driver.host": "127.0.0.1",
    "spark.driver.memory": "1g",
    "spark.python.worker.reuse": "true",
    "spark.sql.execution.arrow.pyspark.enabled": "true",
}


def build_spark_session(
    master: str = "local[2]",
    app_name: str = "sparkquest",
    extra: dict | None = None,
    delta: bool = False,
) -> SparkSession:
    """Build (or fetch) a configured SparkSession. Set ``delta=True`` to enable
    Delta Lake (adds the Delta extension, catalog, and JAR package)."""
    builder = SparkSession.builder.master(master).appName(app_name)
    for key, value in _DEFAULT_CONF.items():
        builder = builder.config(key, value)
    if delta:
        builder = (
            builder.config("spark.jars.packages", DELTA_PACKAGE)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
    for key, value in (extra or {}).items():
        builder = builder.config(key, str(value))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
