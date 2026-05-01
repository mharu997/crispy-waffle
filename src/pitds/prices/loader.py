"""Price loader orchestrator: Tiingo → yfinance → Stooq cascade.

For each ticker, we try sources in order and keep the first non-empty
response. Each successful row is tagged with ``source`` so downstream
analysis can audit the provenance mix.

The cascade is per-ticker, not per-row. If Tiingo returns 5 years and
the ticker existed for 10, we accept the gap rather than splicing —
splicing across sources risks subtle adjustments that don't agree.
Phase B's ``reconcile.py`` adds cross-source agreement checks.
"""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Callable, Iterable

import pandas as pd
import pyarrow as pa

from ..config import settings
from ..store import schemas, writer
from . import stooq, tiingo, yfinance_loader

log = logging.getLogger(__name__)


SourceFunc = Callable[..., pd.DataFrame]


def cascade_sources() -> list[tuple[str, SourceFunc]]:
    """Return the list of (name, fetch_fn) in priority order.

    Tiingo only included if ``TIINGO_API_KEY`` is configured.
    """
    sources: list[tuple[str, SourceFunc]] = []
    if settings.tiingo_api_key:
        sources.append(("tiingo", tiingo.fetch_history))
    sources.append(("yfinance", yfinance_loader.fetch_history))
    sources.append(("stooq", stooq.fetch_history))
    return sources


def fetch_one_ticker(
    ticker: str,
    *,
    start: dt.date,
    end: dt.date,
) -> pd.DataFrame:
    """Try sources in cascade order; return the first non-empty DataFrame.

    Returned DataFrame columns: date, open, high, low, close, volume, source.
    """
    for name, fn in cascade_sources():
        try:
            df = fn(ticker, start=start, end=end)
        except Exception as e:
            log.warning("%s failed for %s: %s", name, ticker, e)
            continue
        if not df.empty:
            df = df.copy()
            df["source"] = name
            log.debug("%s: %s rows from %s", ticker, len(df), name)
            return df
    log.warning("All sources failed/empty for %s", ticker)
    return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "source"])


def fetch_universe(
    tickers: Iterable[str],
    *,
    start: dt.date,
    end: dt.date,
    cik_resolver: Callable[[str], str | None] | None = None,
) -> pa.Table:
    """Fetch prices for *tickers*, return a single Arrow Table conforming to the prices schema."""
    today = dt.date.today()
    rows: list[pd.DataFrame] = []
    tickers = list(tickers)

    for i, ticker in enumerate(tickers, 1):
        if i % 50 == 0:
            log.info("Price backfill progress: %s/%s", i, len(tickers))
        df = fetch_one_ticker(ticker, start=start, end=end)
        if df.empty:
            continue
        df = df.copy()
        df["ticker"] = ticker
        df["cik"] = cik_resolver(ticker) if cik_resolver else None
        df["figi"] = None
        df["ingestion_date"] = today
        rows.append(df)

    if not rows:
        return pa.Table.from_pandas(
            pd.DataFrame(
                columns=[
                    "date", "ticker", "cik", "figi", "open", "high",
                    "low", "close", "volume", "source", "ingestion_date",
                ]
            ),
            schema=schemas.PRICES,
            preserve_index=False,
        )

    combined = pd.concat(rows, ignore_index=True)
    # Order columns to match the schema
    combined = combined[
        [
            "date", "ticker", "cik", "figi", "open", "high",
            "low", "close", "volume", "source", "ingestion_date",
        ]
    ]
    # Coerce nullable cik to PyArrow-friendly type before from_pandas.
    combined["cik"] = combined["cik"].astype("string")
    combined["figi"] = combined["figi"].astype("string")
    return pa.Table.from_pandas(combined, schema=schemas.PRICES, preserve_index=False)


def backfill(
    tickers: Iterable[str],
    *,
    start: dt.date,
    end: dt.date,
    cik_resolver: Callable[[str], str | None] | None = None,
) -> int:
    """Fetch and persist prices for *tickers*; return rowcount written."""
    table = fetch_universe(tickers, start=start, end=end, cik_resolver=cik_resolver)
    if table.num_rows == 0:
        log.warning("No price data fetched; nothing to write")
        return 0
    writer.upsert_single_file(
        table,
        writer.prices_path(),
        primary_key=["ticker", "date"],  # cik may be null; ticker+date uniquely identifies
        sort_by=["date", "ticker"],
    )
    return table.num_rows
