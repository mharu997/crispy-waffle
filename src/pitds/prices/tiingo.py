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
