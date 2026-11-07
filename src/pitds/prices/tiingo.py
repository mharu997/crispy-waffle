"""Tiingo price loader (Tier A / primary).

Tiingo's free tier provides clean, stable EOD US equities from 2000+ with
better delisted-ticker coverage than yfinance and a much more reliable
API. Free tier: 50 unique tickers per hour, 500 per day for IEX, but the
EOD endpoint is unmetered for users with a free account.

Endpoint: https://api.tiingo.com/tiingo/daily/{ticker}/prices
Docs: https://www.tiingo.com/documentation/end-of-day
"""

from __future__ import annotations

import datetime as dt
import logging

import httpx
import pandas as pd
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ..config import settings
from . import symbols

log = logging.getLogger(__name__)

BASE_URL = "https://api.tiingo.com/tiingo/daily"


def _headers() -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Token {settings.require_tiingo()}",
    }


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type(httpx.HTTPError),
    reraise=True,
)
def fetch_history(
    ticker: str,
    *,
    start: dt.date,
    end: dt.date,
) -> pd.DataFrame:
    """Return unadjusted OHLCV for *ticker* in [start, end].

    Tiingo returns ``adjOpen/adjHigh/adjLow/adjClose/adjVolume`` (already
    split-and-dividend-adjusted) and ``open/high/low/close/volume``
    (unadjusted). PITDS stores unadjusted; adjustments computed on read.
    """
    url = f"{BASE_URL}/{symbols.for_tiingo(ticker)}/prices"
    params = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "format": "json",
        "resampleFreq": "daily",
    }
    r = httpx.get(url, headers=_headers(), params=params, timeout=30.0)
    if r.status_code == 404:
        # Tiingo returns 404 for unknown tickers; signal empty.
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    r.raise_for_status()
    data = r.json()
    if not data:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df[["date", "open", "high", "low", "close", "volume"]]


# ---------------------------------------------------------------------------
# Entity-keyed fetch (permaTicker)
# ---------------------------------------------------------------------------
#
# Tiingo's ``/daily/{ticker}/prices`` resolves a ticker to whichever entity
# holds it *today*: ``STI`` returns Solidion Technology (2024+), not SunTrust
# (1987-2019). The same endpoint also accepts a ``permaTicker`` (e.g.
# ``US000000000420`` = SunTrust), which pins the request to one entity for
# its entire life regardless of renames or ticker reuse. permaTickers come
# from ``/tiingo/fundamentals/meta`` (see identifiers/tiingo_meta.py).
#
# The response also carries ``divCash`` and ``splitFactor``, so prices and
# the corporate-action log can be derived from the *same* entity in one call.

ENTITY_COLUMNS = ["date", "open", "high", "low", "close", "volume", "divCash", "splitFactor"]


def _entity_cache_path(key: str):
    return settings.raw_dir / "tiingo" / "eod" / f"{key.upper()}.parquet"


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type(httpx.HTTPError),
    reraise=True,
)
def _get_entity(key: str, start: dt.date, end: dt.date) -> pd.DataFrame:
    url = f"{BASE_URL}/{key}/prices"
    params = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "format": "json",
        "resampleFreq": "daily",
    }
    r = httpx.get(url, headers=_headers(), params=params, timeout=60.0)
    if r.status_code in (400, 404):
        return pd.DataFrame(columns=ENTITY_COLUMNS)
    if r.status_code == 429:
        raise httpx.HTTPError("Tiingo rate limit (429)")
    r.raise_for_status()
    data = r.json()
    if not data:
        return pd.DataFrame(columns=ENTITY_COLUMNS)
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for c in ENTITY_COLUMNS:
        if c not in df.columns:
            df[c] = None
    return df[ENTITY_COLUMNS]


REFRESH_OVERLAP_DAYS = 14  # re-pull this much history on incremental refresh
FINISHED_AFTER_DAYS = 30  # series with no bar for this long before req_end are final


def _write_cache(path, df: pd.DataFrame, start: dt.date, end: dt.date) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    out = df.copy()
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce")
    for c in ("open", "high", "low", "close", "divCash", "splitFactor"):
        out[c] = pd.to_numeric(out[c], errors="coerce")
    tbl = pa.Table.from_pandas(out, preserve_index=False)
    tbl = tbl.replace_schema_metadata(
        {b"req_start": start.isoformat().encode(), b"req_end": end.isoformat().encode()}
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(tbl, path)


def fetch_entity_history(
    key: str,
    *,
    start: dt.date,
    end: dt.date,
    use_cache: bool = True,
) -> pd.DataFrame:
    """Return unadjusted OHLCV + divCash + splitFactor for a permaTicker (or ticker).

    Cached per key under ``data/raw/tiingo/eod/`` with the request window in
    the file metadata. Cache policy:

    * cache covers ``[start, end]`` → served from disk;
    * cached series *finished* (no bar in the last ``FINISHED_AFTER_DAYS``
      before its request end — delisted) → served from disk, never re-pulled;
    * otherwise incremental: re-pull from ``last_bar - REFRESH_OVERLAP_DAYS``
      to ``end`` and merge (new rows win), so the nightly job costs one small
      request per active entity instead of a full history.
    """
    import pyarrow.parquet as pq

    path = _entity_cache_path(key)
    if use_cache and path.exists():
        kv = pq.read_schema(path).metadata or {}
        req_start = kv.get(b"req_start", b"").decode()
        req_end = kv.get(b"req_end", b"").decode()
        if req_start and req_end and req_start <= start.isoformat():
            cached = pd.read_parquet(path)
            last_bar = cached["date"].max() if not cached.empty else None
            r_end = dt.date.fromisoformat(req_end)
            finished = last_bar is None or (r_end - last_bar).days > FINISHED_AFTER_DAYS
            if req_end >= end.isoformat() or finished:
                mask = (cached["date"] >= start) & (cached["date"] <= end)
                return cached.loc[mask].reset_index(drop=True)
            fresh = _get_entity(key, last_bar - dt.timedelta(days=REFRESH_OVERLAP_DAYS), end)
            merged = pd.concat([cached.loc[cached["date"] < fresh["date"].min()] if not fresh.empty else cached, fresh])
            merged = merged.drop_duplicates("date", keep="last").sort_values("date").reset_index(drop=True)
            _write_cache(path, merged, dt.date.fromisoformat(req_start), end)
            mask = (merged["date"] >= start) & (merged["date"] <= end)
            return merged.loc[mask].reset_index(drop=True)

    df = _get_entity(key, start, end)
    _write_cache(path, df, start, end)
    return df
