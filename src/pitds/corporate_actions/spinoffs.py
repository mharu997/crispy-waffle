"""Manually-curated S&P 500 spinoffs since 2010.

yfinance does not report spinoffs as a discrete event type — it treats
them as price-history adjustments that may or may not match the spun-off
entity's actual market value. For a PIT-correct dataset we need the
explicit (parent_cik, spinoff_child_cik, ex_date) triple so that
backtests can simulate receiving the spun-off shares.

This is a small hand-curated list. Cross-check sources: company press
releases, SEC 8-K filings, S&P Dow Jones Indices announcements.
"""

from __future__ import annotations

import datetime as dt
import uuid

import pandas as pd
import pyarrow as pa

from ..store import schemas


# Major S&P 500 spinoffs since 2010-01-01.
# Format: (parent_ticker, child_ticker, ex_date, parent_cik, child_cik, notes)
KNOWN_SPINOFFS: list[dict] = [
    # Corrected 2026-11-07: earlier revisions had parent/child reversed for
    # most rows and wrong CIKs for GEV/EMBC. CIKs verified against EDGAR names.
    # 2012
    {
        "parent_ticker": "COP", "child_ticker": "PSX",
        "ex_date": dt.date(2012, 5, 1),
        "parent_cik": "0001163165", "child_cik": "0001534701",
        "notes": "Phillips 66 spun off from ConocoPhillips",
    },
    {
        "parent_ticker": "MDLZ", "child_ticker": "KRFT",
        "ex_date": dt.date(2012, 10, 2),
        "parent_cik": "0001103982", "child_cik": "0001545158",
        "notes": "Kraft Foods Group spun off from Kraft Foods Inc. (renamed Mondelez)",
    },
    # 2013
    {
        "parent_ticker": "ABT", "child_ticker": "ABBV",
        "ex_date": dt.date(2013, 1, 2),
        "parent_cik": "0000001800", "child_cik": "0001551152",
        "notes": "AbbVie spun off from Abbott Laboratories",
    },
    # 2015
    {
        "parent_ticker": "EBAY", "child_ticker": "PYPL",
        "ex_date": dt.date(2015, 7, 20),
        "parent_cik": "0001065088", "child_cik": "0001633917",
        "notes": "PayPal spun off from eBay",
    },
    {
        "parent_ticker": "HPQ", "child_ticker": "HPE",
        "ex_date": dt.date(2015, 11, 2),
        "parent_cik": "0000047217", "child_cik": "0001645590",
        "notes": "Hewlett Packard Enterprise spun off from Hewlett-Packard (renamed HP Inc.)",
    },
    # 2019
    {
        "parent_ticker": "DWDP", "child_ticker": "DOW",
        "ex_date": dt.date(2019, 4, 2),
        "parent_cik": "0001666700", "child_cik": "0001751788",
        "notes": "Dow Inc. spun off from DowDuPont",
    },
    {
        "parent_ticker": "DD", "child_ticker": "CTVA",
        "ex_date": dt.date(2019, 6, 3),
        "parent_cik": "0001666700", "child_cik": "0001755672",
        "notes": "Corteva spun off from DowDuPont (renamed DuPont de Nemours)",
    },
    # 2022
    {
        "parent_ticker": "BDX", "child_ticker": "EMBC",
        "ex_date": dt.date(2022, 4, 1),
        "parent_cik": "0000010795", "child_cik": "0001872789",
        "notes": "Embecta spun off from Becton Dickinson",
    },
    {
        "parent_ticker": "T", "child_ticker": "WBD",
        "ex_date": dt.date(2022, 4, 11),
        "parent_cik": "0000732717", "child_cik": "0001437107",
        "notes": "WarnerMedia spun off from AT&T and merged into Discovery (WBD)",
    },
    # 2023
    {
        "parent_ticker": "GE", "child_ticker": "GEHC",
        "ex_date": dt.date(2023, 1, 4),
        "parent_cik": "0000040545", "child_cik": "0001932393",
        "notes": "GE HealthCare spun off from General Electric",
    },
    # 2024
    {
        "parent_ticker": "GE", "child_ticker": "GEV",
        "ex_date": dt.date(2024, 4, 2),
        "parent_cik": "0000040545", "child_cik": "0001996810",
        "notes": "GE Vernova spun off from General Electric",
    },
]


def build_spinoffs_table() -> pa.Table:
    """Materialize the curated spinoff list into a corporate_actions Arrow Table."""
    today = dt.date.today()
    rows = []
    for s in KNOWN_SPINOFFS:
        rows.append(
            {
                "action_id": str(uuid.uuid4()),
                "cik": s["parent_cik"],
                "ticker": s["parent_ticker"],
                "ex_date": s["ex_date"],
                "action_type": "spinoff",
                "ratio": None,
                "amount": None,
                "spinoff_child_cik": s["child_cik"],
                "source": "manual_curated",
                "ingestion_date": today,
                "perma_ticker": None,
            }
        )
    df = pd.DataFrame(rows)
    df["cik"] = df["cik"].astype("string")
    df["spinoff_child_cik"] = df["spinoff_child_cik"].astype("string")
    df["perma_ticker"] = df["perma_ticker"].astype("string")
    return pa.Table.from_pandas(
        df, schema=schemas.CORPORATE_ACTIONS, preserve_index=False
    )
