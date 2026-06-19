"""Shared pytest configuration."""
import os

# Pin Spark to loopback for any test that starts a session (CI/containers).
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
