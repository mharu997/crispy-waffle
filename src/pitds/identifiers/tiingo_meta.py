"""Tiingo fundamentals-meta: the permaTicker directory.

``GET /tiingo/fundamentals/meta`` (no ``tickers`` param) returns one row
per *entity* Tiingo has ever covered — ~20K rows, ~12K of them inactive.
Each row has a stable ``permaTicker`` plus the entity's latest ``ticker``
and ``name``. Reused tickers therefore appear more than once::

    permaTicker      ticker  name                       isActive
    US000000000420   sti     SunTrust Banks Inc         False
    US000000115138   sti     Solidion Technology Inc    True

That is exactly the disambiguation the price loader needs. (The
``secFilingWebsite`` field, which embeds the CIK, is gated behind the
fundamentals add-on, so CIK resolution happens separately via EDGAR.)

The file is cached under ``data/raw/tiingo/meta/`` and refreshed on demand.
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache

import httpx
import pandas as pd

from ..config import settings

log = logging.getLogger(__name__)

META_URL = "https://api.tiingo.com/tiingo/fundamentals/meta"


def _cache_path():
    return settings.raw_dir / "tiingo" / "meta" / "fundamentals_meta.json"


def fetch_meta(*, refresh: bool = False) -> pd.DataFrame:
    """Download (or load cached) Tiingo fundamentals meta as a DataFrame."""
    path = _cache_path()
    if refresh or not path.exists():
        log.info("Downloading Tiingo fundamentals meta")
        r = httpx.get(
            META_URL,
            headers={"Authorization": f"Token {settings.require_tiingo()}"},
            timeout=300.0,
        )
        r.raise_for_status()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(r.content)
    df = pd.DataFrame(json.loads(path.read_text()))
    df["ticker_u"] = df["ticker"].astype("string").str.upper()
    return df


@lru_cache(maxsize=1)
def load_meta() -> pd.DataFrame:
    return fetch_meta()


def candidates_for(ticker: str) -> pd.DataFrame:
    """All Tiingo entities whose latest ticker equals *ticker* (dash-form)."""
    meta = load_meta()
    tq = ticker.upper().replace(".", "-")
    return meta.loc[meta["ticker_u"] == tq]
