"""Public PIT query interface.

The contract: every function takes an ``as_of`` date and returns only
data that was knowable on that date. There is no global state; callers
pass dates explicitly.

Phase A surface:

* :func:`constituents` — universe on a date.
* :func:`prices` — closing prices for a universe on a date.
* :func:`price_history` — full price history through a date.

Phase B will add :func:`fact` and :func:`sector` once fundamentals and
sector_history are populated.
"""

from __future__ import annotations

import datetime as dt
from typing import Literal

import duckdb
import pandas as pd

from ..fundamentals import pit_layer as _funds_pit
from ..prices.adjust import adjusted_prices_query
from ..store import writer

AdjustMode = Literal["none", "split", "split_div"]
VintageMode = Literal["latest_known", "first_reported"]


def _con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


def constituents(as_of: dt.date, *, announce_aware: bool = False) -> pd.DataFrame:
    """Return the index members on *as_of*.

    Returned columns: ticker, cik, figi.

    ``announce_aware`` is a Phase B flag; it requires the constituents
    pipeline to record announcement dates separately from effective dates.
    For Phase A the two are equal so the flag is a no-op.
    """
    path = writer.constituents_daily_path()
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found — run `pitds backfill-constituents`"
        )
    con = _con()
    # Round down to the most recent trading day on or before *as_of*.
    # This matches the practical semantics of "what did the world look
    # like at as_of?" — on a non-trading day, the answer is the close
    # of the previous trading session.
    effective_date = con.sql(
        f"""
        SELECT MAX(date)
        FROM read_parquet({str(path)!r})
        WHERE date <= DATE '{as_of.isoformat()}'
        """
    ).fetchone()[0]
    if effective_date is None:
        con.close()
        return pd.DataFrame(columns=["ticker", "cik", "figi"])

    # Join ticker_history at read time to populate CIK; constituents_daily
    # itself has CIK as nullable and unpopulated by the materializer.
    th_path = writer.ticker_history_path()
    if th_path.exists():
        df = con.sql(
            f"""
            SELECT
                c.ticker,
                COALESCE(c.cik, th.cik) AS cik,
                c.figi
            FROM read_parquet({str(path)!r}) c
            LEFT JOIN read_parquet({str(th_path)!r}) th
                   ON th.ticker = c.ticker
                  AND th.valid_from <= c.date
                  AND (th.valid_to IS NULL OR th.valid_to > c.date)
            WHERE c.date = DATE '{effective_date.isoformat()}'
              AND c.announced_only = FALSE
            ORDER BY c.ticker
            """
        ).df()
    else:
        df = con.sql(
            f"""
            SELECT ticker, cik, figi
            FROM read_parquet({str(path)!r})
            WHERE date = DATE '{effective_date.isoformat()}'
              AND announced_only = FALSE
            ORDER BY ticker
            """
        ).df()
    con.close()
    return df


def prices(
    as_of: dt.date,
    *,
    cik: str | None = None,
    ticker: str | None = None,
    adjust: AdjustMode = "split_div",
) -> pd.DataFrame:
    """Return prices for one ticker (or one CIK) on *as_of*.

    If neither ``cik`` nor ``ticker`` is given, returns prices for the
    full constituent universe on that date.
    """
    p_path = writer.prices_path()
    a_path = writer.corporate_actions_path()
    if not p_path.exists():
        raise FileNotFoundError(f"{p_path} not found — run `pitds backfill-prices`")

    sql = adjusted_prices_query(
        prices_path=str(p_path),
        actions_path=str(a_path) if a_path.exists() else None,
        mode=adjust,
    )
    con = _con()
    where = [f"date = DATE '{as_of.isoformat()}'"]
    if cik:
        where.append(f"cik = '{cik}'")
    if ticker:
        where.append(f"ticker = '{ticker.upper()}'")
    where_clause = " AND ".join(where)

    df = con.sql(
        f"""
        SELECT * FROM ({sql})
        WHERE {where_clause}
        ORDER BY ticker
        """
    ).df()
    con.close()
    return df


def price_history(
    *,
    cik: str | None = None,
    ticker: str | None = None,
    start: dt.date | None = None,
    end: dt.date,
    adjust: AdjustMode = "split_div",
) -> pd.DataFrame:
    """Return the full price history for one ticker through ``end``.

    The PIT cutoff: only rows with ``date <= end`` are returned, and
    adjustments are computed using only actions with ``ex_date <= end``.
    """
    p_path = writer.prices_path()
    a_path = writer.corporate_actions_path()
    if not p_path.exists():
        raise FileNotFoundError(f"{p_path} not found — run `pitds backfill-prices`")

    sql = adjusted_prices_query(
        prices_path=str(p_path),
        actions_path=str(a_path) if a_path.exists() else None,
        mode=adjust,
    )
    con = _con()
    where = [f"date <= DATE '{end.isoformat()}'"]
    if start:
        where.append(f"date >= DATE '{start.isoformat()}'")
    if cik:
        where.append(f"cik = '{cik}'")
    if ticker:
        where.append(f"ticker = '{ticker.upper()}'")
    where_clause = " AND ".join(where)

    df = con.sql(
        f"""
        SELECT * FROM ({sql})
        WHERE {where_clause}
        ORDER BY ticker, date
        """
    ).df()
    con.close()
    return df


def fact(
    *,
    cik: str,
    concept: str | None = None,
    field_name: str | None = None,
    period_end: dt.date,
    as_of: dt.date,
    period_start: dt.date | None = None,
    vintage: VintageMode = "latest_known",
) -> pd.DataFrame:
    """Return a single PIT-correct fact for (cik, concept-or-field, period_end).

    Pass either ``concept`` (a fully qualified XBRL concept like
    ``us-gaap:Revenues``) or ``field_name`` (a canonical field like
    ``revenue`` — see fundamentals/concepts.py for the list).

    ``vintage`` controls restatement handling: ``latest_known`` returns
    the most-recently-filed row whose ``filed <= as_of``; ``first_reported``
    returns the first time this period was reported.
    """
    if (concept is None) == (field_name is None):
        raise ValueError("Pass exactly one of concept or field_name")
    if concept is not None:
        return _funds_pit.fact(
            cik=cik, concept=concept, period_end=period_end,
            as_of=as_of, period_start=period_start, vintage=vintage,
        )
    return _funds_pit.field(
        cik=cik, field_name=field_name, period_end=period_end,
        as_of=as_of, vintage=vintage,
    )


def sector(
    *,
    cik: str,
    as_of: dt.date,
    granularity: Literal["sic", "gics_sector"] = "gics_sector",
) -> dict | None:
    """Return the (sic, gics_sector) classification for *cik* on *as_of*.

    Returns None if no sector_history row covers the date.
    """
    path = writer.sector_history_path()
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found — run `pitds backfill-sectors`"
        )
    cik = str(cik).zfill(10)
    con = _con()
    df = con.sql(
        f"""
        SELECT * FROM read_parquet({str(path)!r})
        WHERE cik = '{cik}'
          AND valid_from <= DATE '{as_of.isoformat()}'
          AND (valid_to IS NULL OR valid_to > DATE '{as_of.isoformat()}')
        ORDER BY valid_from DESC
        LIMIT 1
        """
    ).df()
    con.close()
    if df.empty:
        return None
    row = df.iloc[0]
    if granularity == "sic":
        return {"sic_code": int(row["sic_code"]) if pd.notna(row["sic_code"]) else None}
    # Prefer Wikipedia-observed GICS Sector (accurate but approximate PIT)
    # over the SIC crosswalk reconstruction (rigorous PIT but coarse).
    observed = row.get("gics_sector_observed")
    if pd.notna(observed) and observed:
        sector = observed
        source = "observed"
    else:
        sector = row["gics_sector_recon"]
        source = "sic_recon"
    return {
        "gics_sector": sector,
        "gics_sector_source": source,
        "sic_code": int(row["sic_code"]) if pd.notna(row["sic_code"]) else None,
    }
