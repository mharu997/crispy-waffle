"""yfinance price loader (Tier B / fallback).

yfinance is unreliable but unkeyed. We use it as a fallback when Tiingo
isn't configured or returns empty data, and as the source for a few
delisted tickers that Tiingo doesn't carry.

We always request *unadjusted* OHLCV (yfinance's ``auto_adjust=False``)
because PITDS computes adjustments on read from the corporate-action log.
This decouples our adjustments from yfinance's (which has changed
behavior multiple times across versions).
"""

from __future__ import annotations

import datetime as dt
import logging
from decimal import Decimal

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

from . import symbols

log = logging.getLogger(__name__)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def fetch_history(
    ticker: str,
    *,
    start: dt.date,
    end: dt.date,
) -> pd.DataFrame:
    """Return unadjusted OHLCV for *ticker* in [start, end).

    Columns: date, open, high, low, close, volume. Empty DataFrame if
    yfinance has no data (delisted, network blocked, etc.).
    """
    import yfinance as yf

    # yfinance's end is exclusive on close; pad by one day to be safe.
    end_pad = end + dt.timedelta(days=1)
    raw = yf.download(
        symbols.for_yfinance(ticker),
        start=start.isoformat(),
        end=end_pad.isoformat(),
        auto_adjust=False,  # we want UNADJUSTED; we adjust on read
        actions=False,  # actions fetched separately by corporate_actions module
        progress=False,
        threads=False,  # safer for sequential rate-limit handling
    )
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    # yfinance returns a MultiIndex columns DataFrame when given one ticker
    # in newer versions (post 0.2.40-ish). Normalize.
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    df = raw.reset_index().rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df[["date", "open", "high", "low", "close", "volume"]]
