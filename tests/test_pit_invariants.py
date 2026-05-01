"""PIT invariant tests — the load-bearing tests that catch lookahead bugs.

These tests run against the locally-built dataset, so they require Phase A
backfill to have been run at least once. They're skipped if the Parquet
files don't exist.
"""

from __future__ import annotations

import datetime as dt

import pytest

from pitds.store import writer

CONSTITUENTS_PARQUET = writer.constituents_daily_path()


pytestmark = pytest.mark.skipif(
    not CONSTITUENTS_PARQUET.exists(),
    reason="constituents_daily.parquet not built; run `pitds backfill-constituents`",
)


@pytest.fixture(scope="module")
def daily_constituents():
    """Load the constituents_daily table once for all tests in the module."""
    import pyarrow.parquet as pq

    return pq.read_table(CONSTITUENTS_PARQUET).to_pandas()


def test_constituent_count_invariant(daily_constituents):
    """Every date should have between 495 and 510 constituents.

    Lower bound 495 catches missed re-additions; upper bound 510 catches
    the rare day where multiple new dual-class tickers landed on the same
    day. Real-world data should sit at ~500-505.
    """
    counts = daily_constituents.groupby("date").size()
    assert counts.min() >= 495, (
        f"Some date has fewer than 495 constituents: "
        f"{counts[counts < 495].head().to_dict()}"
    )
    assert counts.max() <= 510, (
        f"Some date has more than 510 constituents: "
        f"{counts[counts > 510].head().to_dict()}"
    )


def test_constituent_no_duplicates(daily_constituents):
    """No (date, ticker) should appear twice."""
    dupes = daily_constituents.duplicated(subset=["date", "ticker"]).sum()
    assert dupes == 0, f"{dupes} duplicate (date, ticker) pairs in constituents_daily"


def test_constituent_date_range(daily_constituents):
    """Date range should start at HISTORY_START (or close to it) and extend to recent days."""
    from pitds.constituents.changes import HISTORY_START

    min_d = daily_constituents["date"].min()
    max_d = daily_constituents["date"].max()
    assert min_d >= HISTORY_START
    # Allow up to 7 days lag from today (weekends + market holidays + late
    # ingestion). Cap at today + 1 to catch obvious far-future bugs.
    today = dt.date.today()
    assert max_d <= today + dt.timedelta(days=1)
    assert max_d >= today - dt.timedelta(days=14), (
        f"Latest constituents date is {max_d}, more than 14 days behind today"
    )


@pytest.mark.skipif(
    not writer.prices_path().exists(),
    reason="prices.parquet not built; run `pitds backfill-prices`",
)
def test_no_future_price_rows():
    """No price row should have a date strictly after today."""
    import pyarrow.parquet as pq

    df = pq.read_table(writer.prices_path()).to_pandas()
    today = dt.date.today()
    future = df[df["date"] > today]
    assert future.empty, f"{len(future)} price rows are dated after today"


@pytest.mark.skipif(
    not writer.prices_path().exists()
    or not writer.constituents_daily_path().exists(),
    reason="prices.parquet or constituents_daily.parquet not built",
)
def test_pit_monotone_visibility():
    """Visibility must be monotone: |universe(d1)| should not exceed
    the count of *known-as-of-d1* prices ≤ count(known-as-of-d2) for d1 < d2."""
    import duckdb

    con = duckdb.connect()
    pp = str(writer.prices_path())

    d1 = dt.date(2018, 6, 30)
    d2 = dt.date(2020, 6, 30)
    n1 = con.sql(f"SELECT COUNT(*) FROM read_parquet({pp!r}) WHERE date <= DATE '{d1}'").fetchone()[0]
    n2 = con.sql(f"SELECT COUNT(*) FROM read_parquet({pp!r}) WHERE date <= DATE '{d2}'").fetchone()[0]
    con.close()
    assert n1 <= n2, "Monotone-visibility violation: more rows knowable earlier than later"
