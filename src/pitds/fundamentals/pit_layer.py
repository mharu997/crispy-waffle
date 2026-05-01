"""Vintage-aware fundamentals retrieval — the heart of PIT semantics.

The PIT contract for fundamentals:

    Given (cik, concept, period_end) and an ``as_of`` date, return the value
    that was knowable on ``as_of`` — i.e. the row with ``filed <= as_of``,
    selected by the chosen vintage policy.

Two vintage policies are supported:

* ``latest_known`` (default): the most recently-filed row whose ``filed
  <= as_of``. This is what you want for "best knowledge of the past as of
  date T" — including any restatements that had been published by then.

* ``first_reported``: the first time this (cik, concept, period_*) tuple
  was filed. This is "as originally reported" semantics, useful for
  research that wants to avoid restatement-driven look-ahead.

Note that EDGAR does not flag restatements explicitly. We infer them from
the existence of multiple rows with the same (cik, concept, period_start,
period_end) and different accession numbers / filed dates.
"""

from __future__ import annotations

import datetime as dt
from typing import Literal

import duckdb
import pandas as pd

from ..store import writer
from . import concepts as concepts_module

VintageMode = Literal["latest_known", "first_reported"]


def _con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


def _facts_glob() -> str:
    """Return a glob expression covering all fundamentals_facts partitions."""
    return str(writer.fundamentals_facts_dir() / "**" / "*.parquet")


def fact(
    *,
    cik: str,
    concept: str,
    period_end: dt.date,
    as_of: dt.date,
    period_start: dt.date | None = None,
    vintage: VintageMode = "latest_known",
) -> pd.DataFrame:
    """Return the (1-row, possibly empty) DataFrame for one (cik, concept, period).

    Filtered by ``filed <= as_of``; ranked by ``filed`` according to *vintage*.
    """
    cik = str(cik).zfill(10)
    order = "DESC" if vintage == "latest_known" else "ASC"

    period_start_clause = (
        f"AND period_start = DATE '{period_start.isoformat()}'"
        if period_start is not None
        else "AND period_start IS NULL"
    )

    sql = f"""
    SELECT *
    FROM read_parquet({_facts_glob()!r}, hive_partitioning = true)
    WHERE cik = '{cik}'
      AND concept = '{concept}'
      AND period_end = DATE '{period_end.isoformat()}'
      {period_start_clause}
      AND filed <= DATE '{as_of.isoformat()}'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cik, concept, period_start, period_end
        ORDER BY filed {order}
    ) = 1
    """
    con = _con()
    df = con.sql(sql).df()
    con.close()
    return df


def field(
    *,
    cik: str,
    field_name: str,
    period_end: dt.date,
    as_of: dt.date,
    vintage: VintageMode = "latest_known",
) -> pd.DataFrame:
    """Return the value for a canonical field (e.g. "revenue") with concept fallback.

    Tries each concept in priority order (see concepts.py); returns the
    first non-empty result. Useful when you don't want to think about
    XBRL taxonomy versions for fields like revenue/net_income/total_assets.
    """
    cik = str(cik).zfill(10)
    candidates = concepts_module.concepts_for(field_name)
    for concept in candidates:
        # Try with period_start=NULL (instantaneous) and with period_start
        # spanning a fiscal year (duration). We don't know which kind ahead
        # of time, so search both.
        for period_start in (None, "any"):
            sql_period_start = (
                "AND period_start IS NULL" if period_start is None else "AND period_start IS NOT NULL"
            )
            order = "DESC" if vintage == "latest_known" else "ASC"
            con = _con()
            sql = f"""
            SELECT *
            FROM read_parquet({_facts_glob()!r}, hive_partitioning = true)
            WHERE cik = '{cik}'
              AND concept = '{concept}'
              AND period_end = DATE '{period_end.isoformat()}'
              {sql_period_start}
              AND filed <= DATE '{as_of.isoformat()}'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY cik, concept, period_start, period_end
                ORDER BY filed {order}
            ) = 1
            """
            df = con.sql(sql).df()
            con.close()
            if not df.empty:
                return df
    return pd.DataFrame()


def all_facts_as_of(
    *,
    cik: str,
    as_of: dt.date,
    vintage: VintageMode = "latest_known",
) -> pd.DataFrame:
    """Return ALL facts knowable on *as_of* for a single CIK (one row per (concept, period))."""
    cik = str(cik).zfill(10)
    order = "DESC" if vintage == "latest_known" else "ASC"
    sql = f"""
    SELECT *
    FROM read_parquet({_facts_glob()!r}, hive_partitioning = true)
    WHERE cik = '{cik}'
      AND filed <= DATE '{as_of.isoformat()}'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cik, concept, period_start, period_end
        ORDER BY filed {order}
    ) = 1
    """
    con = _con()
    df = con.sql(sql).df()
    con.close()
    return df
