"""Project configuration: paths and API keys loaded from environment.

Single source of truth for filesystem layout. All modules should reference
``settings`` rather than hard-coding paths, so that the project can be
relocated by setting ``PITDS_DATA_DIR``.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load .env from the project root (the directory containing pyproject.toml).
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


class Settings(BaseSettings):
    """Environment-loaded settings for PITDS."""

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # API keys
    tiingo_api_key: str = Field(default="", alias="TIINGO_API_KEY")
    openfigi_api_key: str = Field(default="", alias="OPENFIGI_API_KEY")
    sec_user_agent: str = Field(default="", alias="SEC_USER_AGENT")

    # Filesystem
    data_dir: Path = Field(
        default_factory=lambda: Path(os.environ.get("PITDS_DATA_DIR", PROJECT_ROOT / "data")),
        alias="PITDS_DATA_DIR",
    )

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def processed_dir(self) -> Path:
        return self.data_dir / "processed"

    @property
    def fundamentals_dir(self) -> Path:
        return self.processed_dir / "fundamentals_facts"

    def require_tiingo(self) -> str:
        if not self.tiingo_api_key:
            raise RuntimeError(
                "TIINGO_API_KEY is not set. Sign up free at https://www.tiingo.com/"
                " and add the key to .env."
            )
        return self.tiingo_api_key

    def require_sec_user_agent(self) -> str:
        if not self.sec_user_agent:
            raise RuntimeError(
                "SEC_USER_AGENT is not set. Add a line like\n"
                '  SEC_USER_AGENT="Your Name your.email@example.com"\n'
                "to .env. Required by EDGAR per their fair-access policy:\n"
                "https://www.sec.gov/os/accessing-edgar-data"
            )
        return self.sec_user_agent

    def ensure_directories(self) -> None:
        """Create the canonical directory tree if it doesn't exist."""
        for sub in (
            self.raw_dir / "fja05680",
            self.raw_dir / "wikipedia",
            self.raw_dir / "tiingo",
            self.raw_dir / "yfinance",
            self.raw_dir / "stooq",
            self.raw_dir / "edgar" / "companyfacts",
            self.raw_dir / "edgar" / "submissions",
            self.processed_dir,
            self.fundamentals_dir,
        ):
            sub.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
