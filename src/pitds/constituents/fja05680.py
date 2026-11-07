"""Fetch and parse the fja05680/sp500 community-maintained S&P 500 history.

The repository at https://github.com/fja05680/sp500 publishes three useful CSVs:

* ``sp500_ticker_start_end.csv`` — one row per (ticker, tenure interval).
  A ticker can appear multiple times if it left and re-joined the index.
  Half-open intervals: ``start_date <= day < end_date``;
  ``end_date`` empty means "currently a member".

* ``sp500.csv`` — current snapshot with CIK and GICS Sub-Industry.

* ``sp500_changes_since_2019.csv`` — daily add/remove log used for live-edge
  cross-validation between fja05680 monthly refreshes and Wikipedia.

We download the raw files directly from raw.githubusercontent.com and cache
them under ``data/raw/fja05680/`` with the date of fetch in the filename.
"""

from __future__ import annotations

import datetime as dt
import io
import logging
from dataclasses import dataclass
from pathlib import Path

import httpx
import pandas as pd

from ..config import settings
from .corrections import apply_corrections

log = logging.getLogger(__name__)

RAW_BASE = "https://raw.githubusercontent.com/fja05680/sp500/master"

# URL-encoded path for the wide-format file (its filename has & and spaces).
WIDE_FORMAT_PATH = "S%26P%20500%20Historical%20Components%20%26%20Changes.csv"

REQUEST_TIMEOUT = 30.0


@dataclass(frozen=True)
class FjaSnapshot:
    """A locally-cached snapshot of fja05680 data, all three files."""

    ticker_tenures: pd.DataFrame  # ticker, start_date, end_date (corrections applied)
    current_snapshot: pd.DataFrame  # symbol, security, sector, sub_industry, cik, ...
    changes_since_2019: pd.DataFrame  # date, add, remove
    fetched_on: dt.date


def _download(path: str, dest: Path) -> bytes:
    """GET the URL and write the body to *dest*; return the body bytes."""
    url = f"{RAW_BASE}/{path}"
    log.info("Downloading %s → %s", url, dest)
    r = httpx.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True)
    r.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(r.content)
    return r.content


def fetch_snapshot(*, refresh: bool = False) -> FjaSnapshot:
    """Fetch the three fja05680 files; cache by today's date.

    Set ``refresh=True`` to force a re-download even if today's snapshot
    already exists on disk.
    """
    today = dt.date.today()
    base = settings.raw_dir / "fja05680" / today.isoformat()
    base.mkdir(parents=True, exist_ok=True)

    tenures_file = base / "sp500_ticker_start_end.csv"
    current_file = base / "sp500.csv"
    changes_file = base / "sp500_changes_since_2019.csv"

    if refresh or not tenures_file.exists():
        _download("sp500_ticker_start_end.csv", tenures_file)
    if refresh or not current_file.exists():
        _download("sp500.csv", current_file)
    if refresh or not changes_file.exists():
        _download("sp500_changes_since_2019.csv", changes_file)

    tenures = apply_corrections(_parse_tenures(tenures_file.read_bytes()))
    current = _parse_current(current_file.read_bytes())
    changes = _parse_changes(changes_file.read_bytes())

    return FjaSnapshot(tenures, current, changes, today)


def _parse_tenures(raw: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(raw), dtype={"ticker": "string"})
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.date
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce").dt.date
    # Empty end_date means "currently a member"; encode as NaT/None consistently.
    return df[["ticker", "start_date", "end_date"]].reset_index(drop=True)


def _parse_current(raw: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(raw))
    # Normalize column names to snake_case for downstream joins.
    df.columns = [
        c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns
    ]
    # Coerce CIK to zero-padded 10-character string per SEC convention.
    if "cik" in df.columns:
        df["cik"] = (
            pd.to_numeric(df["cik"], errors="coerce")
            .astype("Int64")
            .astype("string")
            .str.zfill(10)
        )
    if "date_added" in df.columns:
        df["date_added"] = pd.to_datetime(df["date_added"], errors="coerce").dt.date
    return df


def _parse_changes(raw: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(raw))
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    # The 'add' and 'remove' fields are comma-separated lists of tickers.
    df["add"] = df["add"].fillna("").astype("string")
    df["remove"] = df["remove"].fillna("").astype("string")
    return df.reset_index(drop=True)


def latest_cached_snapshot() -> FjaSnapshot | None:
    """Return the most recent on-disk snapshot, or None if none exists."""
    root = settings.raw_dir / "fja05680"
    if not root.exists():
        return None
    candidates = sorted(
        (p for p in root.iterdir() if p.is_dir()),
        reverse=True,
    )
    for d in candidates:
        if all(
            (d / f).exists()
            for f in (
                "sp500_ticker_start_end.csv",
                "sp500.csv",
                "sp500_changes_since_2019.csv",
            )
        ):
            log.info("Using cached fja05680 snapshot from %s", d.name)
            return FjaSnapshot(
                ticker_tenures=apply_corrections(
                    _parse_tenures((d / "sp500_ticker_start_end.csv").read_bytes())
                ),
                current_snapshot=_parse_current((d / "sp500.csv").read_bytes()),
                changes_since_2019=_parse_changes(
                    (d / "sp500_changes_since_2019.csv").read_bytes()
                ),
                fetched_on=dt.date.fromisoformat(d.name),
            )
    return None
