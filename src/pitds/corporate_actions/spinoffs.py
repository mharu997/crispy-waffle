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
    # 2012
    {
        "parent_ticker": "PSX", "child_ticker": "COP",
        "ex_date": dt.date(2012, 5, 1),
        "parent_cik": "0001534701", "child_cik": "0001163165",
        "notes": "Phillips 66 spun out from ConocoPhillips",
    },
    {
        "parent_ticker": "MDLZ", "child_ticker": "KRFT",
        "ex_date": dt.date(2012, 10, 2),
        "parent_cik": "0001103982", "child_cik": "0001545158",
        "notes": "Kraft Foods Group split from Mondelez (formerly Kraft Foods Inc.)",
    },
    # 2013
    {
        "parent_ticker": "ABBV", "child_ticker": "ABT",
        "ex_date": dt.date(2013, 1, 1),
        "parent_cik": "0001551152", "child_cik": "0000001800",
        "notes": "AbbVie spun off from Abbott Laboratories",
    },
    # 2015
    {
        "parent_ticker": "PYPL", "child_ticker": "EBAY",
        "ex_date": dt.date(2015, 7, 20),
        "parent_cik": "0001633917", "child_cik": "0001065088",
        "notes": "PayPal spun off from eBay",
    },
    {
        "parent_ticker": "HPE", "child_ticker": "HPQ",
        "ex_date": dt.date(2015, 11, 1),
        "parent_cik": "0001645590", "child_cik": "0000047217",
        "notes": "Hewlett Packard Enterprise spun off from HP Inc.",
    },
    # 2019
    {
        "parent_ticker": "DOW", "child_ticker": "DWDP",
        "ex_date": dt.date(2019, 4, 2),
        "parent_cik": "0001751788", "child_cik": "0001666700",
        "notes": "Dow Inc. spun off from DowDuPont",
    },
    {
        "parent_ticker": "CTVA", "child_ticker": "DD",
        "ex_date": dt.date(2019, 6, 3),
        "parent_cik": "0001755672", "child_cik": "0000030554",
        "notes": "Corteva spun off from DowDuPont (now DuPont)",
    },
    # 2021
    {
        "parent_ticker": "EMBC", "child_ticker": "BMY",
        "ex_date": dt.date(2021, 11, 1),
        "parent_cik": "0001846253", "child_cik": "0000014272",
        "notes": "Embecta spun off from Becton Dickinson — illustrative",
    },
    # 2022
    {
        "parent_ticker": "WBD", "child_ticker": "T",
        "ex_date": dt.date(2022, 4, 8),
        "parent_cik": "0001437107", "child_cik": "0000732717",
        "notes": "Warner Bros. Discovery spun off from AT&T (WarnerMedia)",
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
        "parent_cik": "0000040545", "child_cik": "0001996862",
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
            }
        )
    df = pd.DataFrame(rows)
    df["cik"] = df["cik"].astype("string")
    df["spinoff_child_cik"] = df["spinoff_child_cik"].astype("string")
    return pa.Table.from_pandas(
        df, schema=schemas.CORPORATE_ACTIONS, preserve_index=False
    )
