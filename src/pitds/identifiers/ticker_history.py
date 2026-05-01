"""``ticker_history`` table: (ticker, cik, valid_from, valid_to) intervals.

Tickers are not permanent identifiers — they get reassigned when companies
rename (FB → META, GOOG → ALPHABET via the GOOGL Class A introduction),
re-listed under a different parent, or recycled to entirely different
companies. CIK is the canonical permanent identifier.

This module materializes the ticker→cik mapping with validity windows so
that the price loader can join historical price rows to the right CIK
even after a rename. The Phase A version handles only currently-active
tickers (mapped via SEC's company_tickers.json); historical reassignments
are a Phase B refinement.
"""

from __future__ import annotations

import datetime as dt
import logging

import pandas as pd
import pyarrow as pa

from ..store import schemas, writer
from .cik_map import resolve_ciks

log = logging.getLogger(__name__)


def build_initial_ticker_history(
    tenures: pd.DataFrame,
    *,
    seed_cik: dict[str, str] | None = None,
) -> pa.Table:
    """Build a starter ``ticker_history`` from the constituent tenure log.

    For each (ticker, tenure interval) we attempt CIK resolution and emit a
    row with valid_from = start_date, valid_to = end_date. Unresolved
    tickers (NULL CIK) are skipped — they will be recovered by Phase B's
    OpenFIGI/EDGAR-search pipeline.
    """
    today = dt.date.today()
    unique_tickers = sorted(set(tenures["ticker"].astype(str).str.upper()))
    cik_map = resolve_ciks(unique_tickers, seed=seed_cik)
    resolved = sum(1 for v in cik_map.values() if v is not None)
    log.info(
        "ticker_history: resolved %s/%s tickers via SEC + seed",
        resolved,
        len(unique_tickers),
    )

    rows: list[dict] = []
    for r in tenures.itertuples(index=False):
        ticker = str(r.ticker).upper()
        cik = cik_map.get(ticker)
        if cik is None:
            continue
        rows.append(
            {
                "ticker": ticker,
                "cik": cik,
                "valid_from": r.start_date,
                "valid_to": r.end_date if pd.notna(r.end_date) else None,
                "source": "sec_company_tickers",
                "ingestion_date": today,
            }
        )

    df = pd.DataFrame(rows)
    return pa.Table.from_pandas(
        df, schema=schemas.TICKER_HISTORY, preserve_index=False
    )


def write_initial(
    tenures: pd.DataFrame, *, seed_cik: dict[str, str] | None = None
) -> int:
    """Write the initial ticker_history Parquet; return rowcount."""
    table = build_initial_ticker_history(tenures, seed_cik=seed_cik)
    writer.write_parquet(table, writer.ticker_history_path())
    return table.num_rows
