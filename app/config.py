"""Application configuration via environment variables (12-factor style)."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """Typed, validated settings. Values come from environment or a local .env file."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Web server ---
    host: str = Field("0.0.0.0", validation_alias="SPARKQUEST_HOST")
    port: int = Field(7860, validation_alias="SPARKQUEST_PORT")
    env: str = Field("development", validation_alias="SPARKQUEST_ENV")

    # --- Code execution sandbox ---
    exec_timeout: int = Field(25, validation_alias="SQ_EXEC_TIMEOUT_SECONDS")
    spark_exec_timeout: int = Field(90, validation_alias="SQ_SPARK_EXEC_TIMEOUT_SECONDS")
    max_output_chars: int = Field(20000, validation_alias="SQ_EXEC_MAX_OUTPUT_CHARS")
    spark_master: str = Field("local[2]", validation_alias="SQ_SPARK_MASTER")

    # --- AI Tutor ---
    tutor_provider: str = Field("none", validation_alias="SQ_TUTOR_PROVIDER")
    tutor_model: str = Field("", validation_alias="SQ_TUTOR_MODEL")
    tutor_api_key: str = Field("", validation_alias="SQ_TUTOR_API_KEY")
    tutor_hf_endpoint: str = Field("", validation_alias="SQ_TUTOR_HF_ENDPOINT")

    # --- Content ---
    lessons_dir: str = Field(str(REPO_ROOT / "lessons"), validation_alias="SQ_LESSONS_DIR")

    @property
    def is_production(self) -> bool:
        return self.env.lower() == "production"


@lru_cache
def get_settings() -> Settings:
    return Settings()
