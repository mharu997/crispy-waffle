"""Stooq price loader (Tier C / deep fallback).

Stooq has no API; the `pandas-datareader` library scrapes their public
CSV endpoints. Coverage extends further back than yfinance for many
delisted US tickers, making it the right tool of last resort for
2010–2012 delistings.

Note: Stooq tickers append `.US` for US listings. The reader handles
this transparently.
"""

from __future__ import annotations

import datetime as dt
import logging

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
    """Return unadjusted OHLCV for *ticker* in [start, end] via Stooq."""
    from pandas_datareader.stooq import StooqDailyReader

    reader = StooqDailyReader(symbols=symbols.for_stooq(ticker), start=start, end=end)
    try:
        raw = reader.read()
    except Exception as e:
        log.warning("Stooq returned no data for %s: %s", ticker, e)
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

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
    return df[["date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)
