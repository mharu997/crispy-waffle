"""Unit tests for the identity spine: corrections, segments, labels, adjustment.

Pure-function tests — no network. The adjustment tests build tiny Parquet
files in a tmp dir and run the real DuckDB SQL.
"""

from __future__ import annotations

import datetime as dt
import math
from collections import namedtuple

import duckdb
import pandas as pd
import pytest

from pitds.constituents.corrections import _cut, apply_corrections
from pitds.identifiers.security_master import load_overrides, split_into_segments
from pitds.identifiers.ticker_labels import _curated_rows
from pitds.prices.adjust import adjusted_prices_query

D = dt.date


# ---------------------------------------------------------------------------
# Membership corrections
# ---------------------------------------------------------------------------


def test_cut_middle_splits_interval():
    assert _cut(D(2010, 1, 1), D(2020, 1, 1), D(2015, 3, 23), D(2015, 6, 15)) == [
        (D(2010, 1, 1), D(2015, 3, 23)),
        (D(2015, 6, 15), D(2020, 1, 1)),
    ]


def test_cut_open_ended_tenure_from_date():
    assert _cut(D(2010, 1, 1), None, D(2026, 9, 20), dt.date.max) == [(D(2010, 1, 1), D(2026, 9, 20))]


def _corr(rows):
    return pd.DataFrame(rows, columns=["action", "ticker", "start_date", "end_date", "note"])


def test_apply_corrections_add_and_remove():
    ten = pd.DataFrame(
        {"ticker": ["AGN", "X"], "start_date": [D(1996, 1, 2), D(2011, 1, 1)], "end_date": [D(2020, 5, 12), None]}
    )
    corr = _corr(
        [
            ("add", "ACT", D(2013, 1, 24), D(2015, 6, 15), ""),
            ("remove", "AGN", D(2015, 3, 23), D(2015, 6, 15), ""),
        ]
    )
    out = apply_corrections(ten, corr)
    agn = out[out.ticker == "AGN"].sort_values("start_date")
    assert list(zip(agn.start_date, agn.end_date, strict=True)) == [
        (D(1996, 1, 2), D(2015, 3, 23)),
        (D(2015, 6, 15), D(2020, 5, 12)),
    ]
    assert ((out.ticker == "ACT") & (out.start_date == D(2013, 1, 24))).sum() == 1
    assert out[out.ticker == "X"].end_date.isna().all()


def test_shipped_corrections_restore_known_missing_members():
    """fja05680 omits ACE, TYC and Watson/Actavis; the curated file restores them."""
    ten = pd.DataFrame({"ticker": ["CB"], "start_date": [D(1996, 1, 2)], "end_date": [None]})
    out = apply_corrections(ten, live_edge=False)
    assert {"ACE", "TYC", "WPI", "ACT"} <= set(out.ticker)


# ---------------------------------------------------------------------------
# Entity overrides / segments
# ---------------------------------------------------------------------------


def _ov(rows):
    return pd.DataFrame(rows, columns=["ticker", "valid_from", "valid_to", "tiingo_key", "cik", "note"])


def test_split_into_segments_at_override_boundary():
    ov = _ov(
        [
            ("IR", None, D(2020, 3, 2), "TT", None, ""),
            ("IR", D(2020, 3, 2), None, "IRINC", None, ""),
        ]
    )
    segs = split_into_segments("IR", D(2010, 11, 17), None, ov)
    assert [(a, b, r["tiingo_key"]) for a, b, r in segs] == [
        (D(2010, 11, 17), D(2020, 3, 2), "TT"),
        (D(2020, 3, 2), None, "IRINC"),
    ]


def test_split_clips_to_history_start_and_leaves_gaps_unassigned():
    ov = _ov([("CCE", D(2010, 10, 4), None, "CCEP", None, "")])
    segs = split_into_segments("CCE", D(1998, 10, 8), D(2016, 5, 31), ov)
    assert segs[0][0] == D(2010, 1, 1) and segs[0][1] == D(2010, 10, 4) and segs[0][2] is None
    assert segs[1][2]["tiingo_key"] == "CCEP"


def test_overrides_file_merges_same_range_rows():
    ov = load_overrides()
    # Each (ticker, range) appears once, with price series and CIK merged.
    key = ov["ticker"] + "|" + ov["valid_from"].map(str) + "|" + ov["valid_to"].map(str)
    assert not key.duplicated().any()
    endp = ov[ov.ticker == "ENDP"].iloc[0]
    assert endp["tiingo_key"] and endp["cik"] == "0001593034"


# ---------------------------------------------------------------------------
# Curated as-of labels
# ---------------------------------------------------------------------------


def test_curated_label_rows_split_part():
    P = namedtuple("P", "ticker valid_from valid_to cik")
    p = P("GOOGL", D(2010, 1, 1), D(2015, 10, 2), "0001288776")
    rows = _curated_rows(p, [(D(1900, 1, 1), "GOOG"), (D(2014, 4, 3), "GOOGL")])
    assert [(r["ticker_pit"], r["valid_from"], r["valid_to"]) for r in rows] == [
        ("GOOG", D(2010, 1, 1), D(2014, 4, 3)),
        ("GOOGL", D(2014, 4, 3), D(2015, 10, 2)),
    ]


# ---------------------------------------------------------------------------
# On-read adjustment
# ---------------------------------------------------------------------------


def _write(tmp_path, prices, actions):
    pp, ap = tmp_path / "p.parquet", tmp_path / "a.parquet"
    pd.DataFrame(prices, columns=["date", "ticker", "cik", "figi", "close", "perma_ticker"]).assign(
        open=None, high=None, low=None, volume=1, source="t", ingestion_date=D(2026, 1, 1)
    ).to_parquet(pp)
    pd.DataFrame(actions, columns=["ticker", "perma_ticker", "ex_date", "action_type", "ratio", "amount"]).to_parquet(ap)
    return str(pp), str(ap)


def _adj(pp, ap):
    df = duckdb.sql(adjusted_prices_query(prices_path=pp, actions_path=ap, mode="split_div")).df()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df.set_index("date")


def test_split_after_rename_adjusts_old_ticker_rows(tmp_path):
    """ABC→COR: a split after the rename must still adjust ABC-labelled rows."""
    pp, ap = _write(
        tmp_path,
        [(D(2023, 1, 3), "ABC", "1", None, 100.0, "P1"), (D(2024, 1, 3), "COR", "1", None, 50.0, "P1")],
        [("COR", "P1", D(2023, 6, 1), "split", 2.0, None)],
    )
    a = _adj(pp, ap)
    assert a.loc[D(2023, 1, 3), "close_adj"] == pytest.approx(50.0)
    assert a.loc[D(2024, 1, 3), "close_adj"] == pytest.approx(50.0)


def test_same_day_reverse_split_and_distribution(tmp_path):
    """Motorola 2011: 1:7 reverse split + spin-off quoted in post-split units."""
    pp, ap = _write(
        tmp_path,
        [(D(2011, 1, 3), "MOT", "1", None, 9.11, "P"), (D(2011, 1, 4), "MSI", "1", None, 39.77, "P")],
        [
            ("MSI", "P", D(2011, 1, 4), "split", 1 / 7, None),
            ("MSI", "P", D(2011, 1, 4), "cash_div", None, 27.27),
        ],
    )
    a = _adj(pp, ap)
    expected = 9.11 * 7 * (1 - 27.27 / (9.11 * 7))
    assert a.loc[D(2011, 1, 3), "close_adj"] == pytest.approx(expected, rel=1e-6)
    assert math.isfinite(a.loc[D(2011, 1, 3), "adj_factor"])


def test_impossible_distribution_is_skipped_not_fatal(tmp_path):
    pp, ap = _write(
        tmp_path,
        [(D(2016, 9, 30), "RRD", "1", None, 15.72, "P"), (D(2016, 10, 3), "RRD", "1", None, 23.81, "P")],
        [("RRD", "P", D(2016, 10, 3), "cash_div", None, 25.61)],
    )
    a = _adj(pp, ap)
    assert a.loc[D(2016, 9, 30), "close_adj"] == pytest.approx(15.72)
