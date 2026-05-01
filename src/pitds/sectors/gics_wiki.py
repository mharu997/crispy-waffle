"""GICS Sector observed via Wikipedia revision history.

The current Wikipedia "List of S&P 500 companies" article tags every
constituent with its GICS Sector. By walking the article's revision
history, we can extract historical GICS Sector classifications.

This is the rigorous-but-approximate path: Wikipedia is a public,
free, dated record. Its accuracy lags S&P's official classifications
by days-to-weeks, and it doesn't always reflect mid-period sector
reclassifications, but it's the best free historical source for
GICS Sector — better than the SIC crosswalk for high-level sector
attribution.

Phase B baseline: pull a recent revision and extract the current
sector mapping. Phase C: walk historical revisions to build a
``(ticker, valid_from, valid_to, gics_sector)`` log.
"""

from __future__ import annotations

import datetime as dt
import logging
from io import StringIO

import httpx
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

log = logging.getLogger(__name__)

# Wikipedia revisions API; gives us the page HTML at a specific revision
# (rvprop=content) or a timestamped snapshot (rvstart=ISO-date).
REVISIONS_API = (
    "https://en.wikipedia.org/w/api.php?"
    "action=query&format=json&prop=revisions&titles=List_of_S%26P_500_companies"
    "&rvprop=content|timestamp&rvslots=main"
)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=20),
    reraise=True,
)
def fetch_revision_at(*, on_or_before: dt.date) -> dict | None:
    """Fetch the Wikipedia article state as of *on_or_before*.

    Returns a dict with keys ``timestamp`` and ``wikitext``.
    """
    url = (
        REVISIONS_API
        + f"&rvstart={on_or_before.isoformat()}T23:59:59Z&rvlimit=1"
    )
    headers = {"User-Agent": "PITDS/0.1 (research)"}
    r = httpx.get(url, headers=headers, timeout=30.0)
    r.raise_for_status()
    data = r.json()
    pages = data.get("query", {}).get("pages", {})
    if not pages:
        return None
    page = next(iter(pages.values()))
    revs = page.get("revisions", [])
    if not revs:
        return None
    rev = revs[0]
    return {
        "timestamp": rev.get("timestamp"),
        "wikitext": rev.get("slots", {}).get("main", {}).get("*", ""),
    }


def extract_current_gics_table() -> pd.DataFrame:
    """Pull the live Wikipedia page and parse the constituents+GICS table.

    Returns a DataFrame with columns: symbol, security, gics_sector,
    gics_sub_industry. Reuses the constituents/wikipedia.py fetcher.
    """
    from ..constituents import wikipedia as wiki_mod

    df = wiki_mod.fetch_current_constituents()
    return df[["symbol", "security", "gics_sector", "gics_sub_industry"]]
