"""Live-edge constituent tracking via Wikipedia.

The fja05680 baseline is updated approximately monthly. For changes that
land between fja05680 refreshes (typically a handful per month), we scrape
the live Wikipedia "List of S&P 500 companies" article and diff against the
last known state.

This is a thin Phase A stub; the full implementation (revision-history
walking for historical sector reconstruction) lands in Phase B.
"""

from __future__ import annotations

import logging
from io import StringIO

import httpx
import pandas as pd

log = logging.getLogger(__name__)

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def fetch_current_constituents() -> pd.DataFrame:
    """Scrape the current S&P 500 constituents table from Wikipedia.

    Returns a DataFrame with columns roughly: symbol, security, gics_sector,
    gics_sub_industry, headquarters_location, date_added, cik, founded.
    """
    headers = {
        # Wikipedia returns 403 to default Python user agents; identify ourselves.
        "User-Agent": "PITDS/0.1 (https://github.com/local; data-research)"
    }
    r = httpx.get(WIKI_URL, timeout=30.0, headers=headers, follow_redirects=True)
    r.raise_for_status()
    tables = pd.read_html(StringIO(r.text))
    if not tables:
        raise RuntimeError("Wikipedia returned no tables; page structure may have changed")
    # The first table on the page is the current constituents.
    df = tables[0].copy()
    df.columns = [
        str(c).strip().lower().replace(" ", "_").replace("-", "_")
        for c in df.columns
    ]
    if "cik" in df.columns:
        df["cik"] = (
            pd.to_numeric(df["cik"], errors="coerce")
            .astype("Int64")
            .astype("string")
            .str.zfill(10)
        )
    return df
