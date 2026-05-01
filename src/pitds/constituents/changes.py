"""Fold ticker tenures into a daily constituents matrix.

The fja05680 ``ticker_tenures`` DataFrame has one row per (ticker, tenure
interval). To answer "who was in the index on date T?" efficiently, we
materialize a Cartesian product of trading days × active tenures into
``constituents_daily.parquet``.

Half-open intervals: ``start_date <= day < end_date``. ``end_date`` IS NULL
means "currently a member" — clipped to today + 1.

We use the NYSE trading calendar so days the market was closed don't appear
in the matrix. This makes the table the right size for joining to ``prices``.
"""

from __future__ import annotations

import datetime as dt
import logging
import uuid
from typing import Iterable

import duckdb
import pandas as pd
import pandas_market_calendars as mcal
import pyarrow as pa

from ..store import schemas, writer

log = logging.getLogger(__name__)

# Earliest date supported by the dataset. We start from 2010 because XBRL
# fundamentals don't exist freely before that — keeps the universe coherent
# across layers.
HISTORY_START = dt.date(2010, 1, 1)


def trading_days(start: dt.date, end: dt.date) -> list[dt.date]:
    """Return the NYSE trading days inclusive on both ends."""
    cal = mcal.get_calendar("NYSE")
    schedule = cal.schedule(start_date=start, end_date=end)
    # ``schedule.index`` is a tz-aware DatetimeIndex of session start times.
    return [d.date() for d in schedule.index]


def materialize_daily(
    tenures: pd.DataFrame,
    *,
    start: dt.date = HISTORY_START,
    end: dt.date | None = None,
) -> pa.Table:
    """Produce the ``constituents_daily`` Arrow Table from a tenures DataFrame.

    ``tenures`` columns: ``ticker``, ``start_date``, ``end_date`` (NaT means open).

    Returned columns: ``date``, ``cik``, ``ticker``, ``figi``, ``announced_only``.
    ``cik`` and ``figi`` are NULL at this stage; ``identifiers`` populates them.
    ``announced_only`` is False for all rows; the constituents loader does not
    yet know announcement dates separate from effective dates.
    """
    end = end or dt.date.today()

    # Build a DataFrame of trading days in the requested window.
    days = pd.DataFrame({"date": trading_days(start, end)})
    days["date"] = pd.to_datetime(days["date"]).dt.date

    # Normalize tenure end_date to a non-null sentinel that's always > end.
    sentinel = dt.date(9999, 12, 31)
    t = tenures.copy()
    t["end_clipped"] = t["end_date"].fillna(sentinel)

    con = duckdb.connect()
    con.register("days", days)
    con.register("tenures", t)
    arrow = con.sql(
        """
        SELECT
            d.date AS date,
            t.ticker,
            CAST(NULL AS VARCHAR) AS cik,
            CAST(NULL AS VARCHAR) AS figi,
            FALSE AS announced_only
        FROM days d
        JOIN tenures t
          ON d.date >= t.start_date
         AND d.date <  t.end_clipped
        ORDER BY d.date, t.ticker
        """
    ).fetch_arrow_table()
    con.close()

    # Reconstruct as a Table against the canonical schema so field order,
    # types and nullability all match.
    table = pa.Table.from_arrays(
        [arrow.column(name) for name in schemas.CONSTITUENTS_DAILY.names],
        schema=schemas.CONSTITUENTS_DAILY,
    )
    log.info(
        "constituents_daily: %s rows for [%s, %s]", table.num_rows, start, end
    )
    return table


def build_change_events(
    tenures: pd.DataFrame,
    *,
    cik_lookup: dict[str, str] | None = None,
) -> pa.Table:
    """Convert ``tenures`` into a normalized ``constituent_changes`` event log.

    Each tenure row produces an "add" event at ``start_date`` and (if not
    currently active) a "remove" event at ``end_date``. ``announcement_date``
    is set equal to ``effective_date`` here because fja05680 does not
    distinguish them; the live-edge Wikipedia loader will refine this for
    future events.
    """
    today = dt.date.today()
    rows: list[dict] = []
    cik_lookup = cik_lookup or {}

    for r in tenures.itertuples(index=False):
        ticker = r.ticker
        cik = cik_lookup.get(ticker)

        rows.append(
            {
                "event_id": str(uuid.uuid4()),
                "cik": cik,
                "ticker": ticker,
                "figi": None,
                "event_type": "add",
                "announcement_date": r.start_date,
                "effective_date": r.start_date,
                "old_ticker": None,
                "new_ticker": None,
                "reason": None,
                "source": "fja05680",
                "ingestion_date": today,
                "notes": None,
            }
        )
        if pd.notna(r.end_date):
            rows.append(
                {
                    "event_id": str(uuid.uuid4()),
                    "cik": cik,
                    "ticker": ticker,
                    "figi": None,
                    "event_type": "remove",
                    "announcement_date": r.end_date,
                    "effective_date": r.end_date,
                    "old_ticker": None,
                    "new_ticker": None,
                    "reason": None,
                    "source": "fja05680",
                    "ingestion_date": today,
                    "notes": None,
                }
            )

    df = pd.DataFrame(rows)
    return pa.Table.from_pandas(
        df, schema=schemas.CONSTITUENT_CHANGES, preserve_index=False
    )


def write_constituents_outputs(
    tenures: pd.DataFrame,
    current_snapshot: pd.DataFrame,
    *,
    start: dt.date = HISTORY_START,
    end: dt.date | None = None,
) -> tuple[int, int]:
    """Materialize and persist constituents_daily + constituent_changes.

    Returns (n_daily_rows, n_change_events).
    """
    cik_lookup = _cik_lookup_from_current(current_snapshot)
    daily = materialize_daily(tenures, start=start, end=end)
    events = build_change_events(tenures, cik_lookup=cik_lookup)

    writer.write_parquet(daily, writer.constituents_daily_path())
    writer.write_parquet(events, writer.constituent_changes_path())

    return daily.num_rows, events.num_rows


def _cik_lookup_from_current(current: pd.DataFrame) -> dict[str, str]:
    """Build a {ticker: cik} dict from the current snapshot. CIK is zero-padded."""
    if "symbol" not in current.columns or "cik" not in current.columns:
        return {}
    sub = current[["symbol", "cik"]].dropna()
    return dict(zip(sub["symbol"].astype(str), sub["cik"].astype(str), strict=False))
