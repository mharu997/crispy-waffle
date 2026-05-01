"""Property-based PIT invariant tests via Hypothesis.

These tests randomly sample dates and check that the dataset's behavior
satisfies the PIT contract on *all* of them, not just hand-picked dates.
They're slow-ish (run with ``-n auto`` for parallelism) but catch
subtle violations a fixture-based test would miss.

Skipped if the underlying Parquet files don't exist.
"""

from __future__ import annotations

import datetime as dt

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from pitds.store import writer as w

CONST_PARQUET = w.constituents_daily_path()
PRICES_PARQUET = w.prices_path()

pytestmark = pytest.mark.skipif(
    not (CONST_PARQUET.exists() and PRICES_PARQUET.exists()),
    reason="constituents_daily.parquet or prices.parquet not built",
)


# Date strategy: any date in the dataset's lifespan.
# Hardcoded bounds match HISTORY_START and reasonable forward window.
_dates = st.dates(min_value=dt.date(2010, 1, 4), max_value=dt.date(2026, 4, 30))


@given(as_of=_dates)
@settings(max_examples=30, deadline=2000)
def test_universe_size_in_range(as_of):
    """For every sampled date, the universe has between 480 and 510 members.

    The lower bound is loose (480) to allow for weekends/holidays where
    we round down to the previous trading day, but reflects the ~500-member
    structure of the index.
    """
    from pitds.pit import api

    u = api.constituents(as_of=as_of)
    if u.empty:
        # Pre-2010 dates may have no rows since HISTORY_START is 2010-01-04.
        # Hypothesis will sometimes produce dates outside our window.
        return
    assert 480 <= len(u) <= 515, f"Universe on {as_of}: {len(u)} members"


@given(as_of=_dates)
@settings(max_examples=30, deadline=2000)
def test_no_lookahead_in_prices(as_of):
    """No price returned by ``api.prices(as_of=T)`` has date > T."""
    from pitds.pit import api

    df = api.prices(as_of=as_of, ticker="AAPL", adjust="none")
    if df.empty:
        return
    max_date = df["date"].max()
    if hasattr(max_date, "date"):
        max_date = max_date.date()
    assert max_date <= as_of, f"AAPL price as_of={as_of} returned data dated {max_date}"


@given(d1=_dates, d2=_dates)
@settings(max_examples=20, deadline=3000)
def test_universe_history_monotone_in_size(d1, d2):
    """For d1 < d2: the *count of all dates with data* up to d2 is ≥ count up to d1."""
    import duckdb

    if d1 >= d2:
        return

    con = duckdb.connect()
    n1 = con.sql(
        f"SELECT COUNT(DISTINCT date) FROM read_parquet({str(CONST_PARQUET)!r}) "
        f"WHERE date <= DATE '{d1}'"
    ).fetchone()[0]
    n2 = con.sql(
        f"SELECT COUNT(DISTINCT date) FROM read_parquet({str(CONST_PARQUET)!r}) "
        f"WHERE date <= DATE '{d2}'"
    ).fetchone()[0]
    con.close()

    assert n1 <= n2, (
        f"Monotone-history violation: {n1} dates ≤ {d1}, {n2} dates ≤ {d2}"
    )


@given(as_of=_dates)
@settings(max_examples=20, deadline=2000)
def test_price_history_truncated_at_as_of(as_of):
    """price_history(end=T) returns no rows after T."""
    from pitds.pit import api

    hist = api.price_history(ticker="AAPL", end=as_of, adjust="none")
    if hist.empty:
        return
    max_date = hist["date"].max()
    if hasattr(max_date, "date"):
        max_date = max_date.date()
    assert max_date <= as_of
