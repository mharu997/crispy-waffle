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


def build_from_security_master(sm: pd.DataFrame) -> pa.Table:
    """``ticker_history`` from the security master: as-of ticker → CIK windows.

    Supersedes the Phase A version, which mapped every tenure of a ticker to
    the CIK that holds it *today* — wrong for reused tickers (DOW 2010-2017
    is Dow Chemical, not Dow Inc) and blind to renames (PCLN → BKNG).
    Adjacent windows with the same (ticker, cik) are merged.
    """
    today = dt.date.today()
    s = sm.loc[sm["cik"].notna(), ["ticker_pit", "cik", "valid_from", "valid_to"]].copy()
    s = s.sort_values(["ticker_pit", "valid_from"])
    rows: list[dict] = []
    for r in s.itertuples(index=False):
        vt = r.valid_to if pd.notna(r.valid_to) else None
        if rows and rows[-1]["ticker"] == r.ticker_pit and rows[-1]["cik"] == r.cik and rows[-1]["valid_to"] == r.valid_from:
            rows[-1]["valid_to"] = vt
            continue
        rows.append(
            {
                "ticker": r.ticker_pit,
                "cik": r.cik,
                "valid_from": r.valid_from,
                "valid_to": vt,
                "source": "security_master",
                "ingestion_date": today,
            }
        )
    return pa.Table.from_pandas(pd.DataFrame(rows), schema=schemas.TICKER_HISTORY, preserve_index=False)


def write_from_security_master(sm: pd.DataFrame) -> int:
    table = build_from_security_master(sm)
    writer.write_parquet(table, writer.ticker_history_path())
    return table.num_rows
