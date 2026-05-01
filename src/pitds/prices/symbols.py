"""Per-source ticker symbol normalization.

Different price sources spell class-share tickers differently:

* fja05680, S&P, Wikipedia: ``BRK.B``, ``BF.B``  (dot)
* Tiingo, yfinance, Stooq:  ``BRK-B``, ``BF-B``  (dash)

We canonically store the dot-form (per S&P convention) but rewrite to the
dash-form when querying free price sources. This module is the single
place that knowledge lives.
"""

from __future__ import annotations


def for_tiingo(ticker: str) -> str:
    """Return the ticker in the format Tiingo expects."""
    return ticker.replace(".", "-")


def for_yfinance(ticker: str) -> str:
    """Return the ticker in the format yfinance expects."""
    return ticker.replace(".", "-")


def for_stooq(ticker: str) -> str:
    """Return the ticker in the format Stooq's daily reader expects."""
    # Stooq's StooqDailyReader strips the dot; passing dash is safer.
    return ticker.replace(".", "-")


def canonical(ticker: str) -> str:
    """Return our canonical (dot-form) ticker. Idempotent."""
    return ticker.replace("-", ".") if ticker.count("-") == 1 and len(ticker.split("-")[1]) <= 2 else ticker
