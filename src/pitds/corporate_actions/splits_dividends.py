"""Splits and cash dividends from yfinance / Tiingo.

The action log is the input to ``prices/adjust.py``. We store events as
they occur (no cumulative adjustments here) so that the adjustment
function can compute the right factor for any ``as_of`` date.

For Phase A we use yfinance's actions stream. yfinance's split/dividend
data is reasonably stable across versions and free; Tiingo is a planned
cross-check.
"""

from __future__ import annotations

import datetime as dt
import logging
import uuid
from collections.abc import Iterable

import pandas as pd
import pyarrow as pa
from tenacity import retry, stop_after_attempt, wait_exponential

from ..prices import symbols
from ..store import schemas, writer

log = logging.getLogger(__name__)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=20),
    reraise=True,
)
def fetch_actions_yfinance(ticker: str) -> pd.DataFrame:
    """Return all known split + dividend events for *ticker* from yfinance.

    Columns: ex_date, action_type ('split'|'cash_div'), ratio, amount.
    """
    import yfinance as yf

    t = yf.Ticker(symbols.for_yfinance(ticker))
    splits = t.splits
    divs = t.dividends

    rows: list[dict] = []
    if splits is not None and not splits.empty:
        for ts, ratio in splits.items():
            rows.append(
                {
                    "ex_date": pd.to_datetime(ts).date(),
                    "action_type": "split",
                    "ratio": float(ratio),
                    "amount": None,
                }
            )
    if divs is not None and not divs.empty:
        for ts, amt in divs.items():
            rows.append(
                {
                    "ex_date": pd.to_datetime(ts).date(),
                    "action_type": "cash_div",
                    "ratio": None,
                    "amount": float(amt),
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=["ex_date", "action_type", "ratio", "amount"]
        )
    df = pd.DataFrame(rows).sort_values("ex_date").reset_index(drop=True)
    return df


def build_actions_table(
    tickers: Iterable[str],
    *,
    cik_resolver=None,
) -> pa.Table:
    """Fetch split/dividend events for *tickers*; return an Arrow Table."""
    today = dt.date.today()
    rows: list[dict] = []

    for ticker in tickers:
        try:
            df = fetch_actions_yfinance(ticker)
        except Exception as e:
            log.warning("yfinance actions failed for %s: %s", ticker, e)
            continue
        if df.empty:
            continue
        cik = cik_resolver(ticker) if cik_resolver else None
        for r in df.itertuples(index=False):
            rows.append(
                {
                    "action_id": str(uuid.uuid4()),
                    "cik": cik,
                    "ticker": ticker,
                    "ex_date": r.ex_date,
                    "action_type": r.action_type,
                    "ratio": r.ratio if r.action_type == "split" else None,
                    "amount": r.amount if r.action_type == "cash_div" else None,
                    "spinoff_child_cik": None,
                    "source": "yfinance",
                    "ingestion_date": today,
                }
            )

    if not rows:
        return pa.Table.from_pandas(
            pd.DataFrame(
                columns=[
                    "action_id", "cik", "ticker", "ex_date", "action_type",
                    "ratio", "amount", "spinoff_child_cik", "source", "ingestion_date",
                ]
            ),
            schema=schemas.CORPORATE_ACTIONS,
            preserve_index=False,
        )

    df = pd.DataFrame(rows)
    df["cik"] = df["cik"].astype("string")
    df["spinoff_child_cik"] = df["spinoff_child_cik"].astype("string")
    return pa.Table.from_pandas(
        df, schema=schemas.CORPORATE_ACTIONS, preserve_index=False
    )


def backfill(tickers: Iterable[str], *, cik_resolver=None) -> int:
    """Fetch and persist corporate actions for *tickers*; return rowcount."""
    table = build_actions_table(tickers, cik_resolver=cik_resolver)
    if table.num_rows == 0:
        return 0
    writer.upsert_single_file(
        table,
        writer.corporate_actions_path(),
        primary_key=["ticker", "ex_date", "action_type"],
        sort_by=["ticker", "ex_date"],
    )
    return table.num_rows
