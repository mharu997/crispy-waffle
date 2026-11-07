"""Point-in-time ticker → (name, CIK) snapshots from Wikipedia revision history.

The "List of S&P 500 companies" article has carried a constituents table
(and, since the mid-2010s, a CIK column) for well over a decade. Its revision
history is therefore a free, dated record of *which company held which
ticker* at each point in time — exactly what SEC's current-only
``company_tickers.json`` cannot tell us for renamed, reused or delisted
tickers.

We sample one revision per quarter (the last revision before each quarter
start), parse the first table, and store the union as
``data/raw/wikipedia/snapshots.parquet`` with columns::

    snapshot_date, revid, ticker, security, cik

Only used as *evidence* for the security master (CIK assignment and
wrong-entity detection), never as the membership source of truth
(fja05680 remains that).
"""

from __future__ import annotations

import datetime as dt
import logging
import re
import time
from io import StringIO

import httpx
import pandas as pd

from ..config import settings

log = logging.getLogger(__name__)

API = "https://en.wikipedia.org/w/api.php"
TITLE = "List of S&P 500 companies"
HEADERS = {"User-Agent": "PITDS/0.1 (https://github.com/mharu997/crispy-waffle; research dataset) python-httpx"}


def _cache_dir():
    return settings.raw_dir / "wikipedia" / "revisions"


def snapshot_path():
    return settings.raw_dir / "wikipedia" / "snapshots.parquet"


def _revision_before(ts: dt.datetime, client: httpx.Client) -> tuple[int, str] | None:
    r = client.get(
        API,
        params={
            "action": "query",
            "prop": "revisions",
            "titles": TITLE,
            "rvlimit": 1,
            "rvdir": "older",
            "rvstart": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "rvprop": "ids|timestamp",
            "format": "json",
        },
    )
    r.raise_for_status()
    pages = r.json()["query"]["pages"]
    page = next(iter(pages.values()))
    revs = page.get("revisions") or []
    if not revs:
        return None
    return revs[0]["revid"], revs[0]["timestamp"]


def _revision_html(revid: int, client: httpx.Client) -> str:
    path = _cache_dir() / f"{revid}.html"
    if path.exists():
        return path.read_text()
    r = client.get(
        API,
        params={"action": "parse", "oldid": revid, "prop": "text", "format": "json"},
    )
    r.raise_for_status()
    html = r.json()["parse"]["text"]["*"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html)
    return html


_TICKER_COLS = ("symbol", "ticker symbol", "ticker")
_NAME_COLS = ("security", "company")


def _parse_constituents(html: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(html))
    for t in tables:
        cols = [re.sub(r"\[.*?\]", "", str(c)).strip().lower() for c in t.columns]
        t.columns = cols
        tcol = next((c for c in _TICKER_COLS if c in cols), None)
        ncol = next((c for c in _NAME_COLS if c in cols), None)
        if tcol and ncol and len(t) > 300:
            out = pd.DataFrame(
                {
                    "ticker": t[tcol].astype(str).str.strip().str.upper(),
                    "security": t[ncol].astype(str).str.strip(),
                }
            )
            if "cik" in cols:
                out["cik"] = (
                    pd.to_numeric(t["cik"], errors="coerce").astype("Int64").astype("string").str.zfill(10)
                )
            else:
                out["cik"] = pd.NA
            scol = next((c for c in cols if c.replace("_", " ") == "gics sector"), None)
            icol = next((c for c in cols if "sub" in c and "industry" in c), None)
            out["gics_sector"] = t[scol].astype(str).str.strip() if scol else pd.NA
            out["gics_sub_industry"] = t[icol].astype(str).str.strip() if icol else pd.NA
            return out
    return pd.DataFrame(columns=["ticker", "security", "cik", "gics_sector", "gics_sub_industry"])


def build_snapshots(
    start: dt.date = dt.date(2010, 1, 1),
    end: dt.date | None = None,
    *,
    months_step: int = 3,
) -> pd.DataFrame:
    """Fetch quarterly revisions and write ``snapshots.parquet``."""
    end = end or dt.date.today()
    stamps = []
    d = dt.date(start.year, start.month, 1)
    while d <= end:
        stamps.append(d)
        m = d.month - 1 + months_step
        d = dt.date(d.year + m // 12, m % 12 + 1, 1)
    stamps.append(end)

    frames = []
    seen: set[int] = set()
    with httpx.Client(headers=HEADERS, timeout=60.0) as client:
        for s in stamps:
            rev = _revision_before(dt.datetime.combine(s, dt.time()), client)
            if rev is None or rev[0] in seen:
                continue
            revid, ts = rev
            seen.add(revid)
            try:
                df = _parse_constituents(_revision_html(revid, client))
            except Exception as e:  # malformed historical revision
                log.warning("Wikipedia revision %s (%s) unparseable: %s", revid, ts, e)
                continue
            df["snapshot_date"] = dt.date.fromisoformat(ts[:10])
            df["revid"] = revid
            frames.append(df)
            log.info("wiki %s rev %s: %s rows, cik=%s", ts[:10], revid, len(df), df["cik"].notna().sum())
            time.sleep(0.2)

    out = pd.concat(frames, ignore_index=True)[
        ["snapshot_date", "revid", "ticker", "security", "cik", "gics_sector", "gics_sub_industry"]
    ]
    out["ticker"] = out["ticker"].str.replace("-", ".", regex=False)
    out.to_parquet(snapshot_path(), index=False)
    return out


def load_snapshots() -> pd.DataFrame:
    p = snapshot_path()
    if not p.exists():
        return build_snapshots()
    return pd.read_parquet(p)


# ---------------------------------------------------------------------------
# Precise label-change dates by bisecting revisions
# ---------------------------------------------------------------------------


def _revisions_between(older: dt.date, newer: dt.date, client: httpx.Client) -> list[tuple[int, str]]:
    """All (revid, timestamp) of the article in (older, newer], oldest first."""
    out: list[tuple[int, str]] = []
    params = {
        "action": "query",
        "prop": "revisions",
        "titles": TITLE,
        "rvlimit": 500,
        "rvdir": "newer",
        "rvstart": f"{older.isoformat()}T00:00:00Z",
        "rvend": f"{newer.isoformat()}T23:59:59Z",
        "rvprop": "ids|timestamp",
        "format": "json",
    }
    while True:
        r = client.get(API, params=params)
        r.raise_for_status()
        js = r.json()
        page = next(iter(js["query"]["pages"].values()))
        out += [(x["revid"], x["timestamp"]) for x in page.get("revisions", [])]
        if "continue" not in js:
            return out
        params.update(js["continue"])


def find_label_change(old: str, new: str, after: dt.date, before: dt.date) -> dt.date | None:
    """Date of the first revision in (after, before] listing *new* instead of *old*.

    Binary search over revisions, so ~log2(N) page parses per change.
    Returns None if the revision list is empty or the change cannot be located.
    """
    with httpx.Client(headers=HEADERS, timeout=60.0) as client:
        revs = _revisions_between(after, before, client)
        if not revs:
            return None

        def has_new(i: int) -> bool | None:
            df = _parse_constituents(_revision_html(revs[i][0], client))
            tickers = set(df["ticker"].str.replace("-", ".", regex=False))
            if new in tickers and old not in tickers:
                return True
            if old in tickers and new not in tickers:
                return False
            return None  # vandalised / transitional revision: treat as unknown

        lo, hi = 0, len(revs) - 1
        if has_new(hi) is not True:
            return None
        while lo < hi:
            mid = (lo + hi) // 2
            v = has_new(mid)
            if v is None:  # step past unknown revisions conservatively
                v = has_new(min(mid + 1, hi))
            if v:
                hi = mid
            else:
                lo = mid + 1
        return dt.date.fromisoformat(revs[lo][1][:10])


def latest_revision(client: httpx.Client | None = None) -> tuple[int, str, pd.DataFrame]:
    """(revid, timestamp, constituents) of the current article revision."""
    own = client is None
    client = client or httpx.Client(headers=HEADERS, timeout=60.0)
    try:
        rev = _revision_before(dt.datetime.utcnow() + dt.timedelta(minutes=5), client)
        if rev is None:
            raise RuntimeError("Wikipedia returned no revisions")
        revid, ts = rev
        df = _parse_constituents(_revision_html(revid, client))
        df["ticker"] = df["ticker"].str.replace("-", ".", regex=False)
        return revid, ts, df
    finally:
        if own:
            client.close()


def find_presence_change(ticker: str, after: dt.date, before: dt.date, *, now_present: bool) -> dt.date | None:
    """First revision date in (after, before] where *ticker* is present (or absent)."""
    with httpx.Client(headers=HEADERS, timeout=60.0) as client:
        revs = _revisions_between(after, before, client)
        if not revs:
            return None

        def ok(i: int) -> bool:
            df = _parse_constituents(_revision_html(revs[i][0], client))
            tickers = set(df["ticker"].str.replace("-", ".", regex=False))
            if len(tickers) < 450:  # vandalised/blanked revision
                return False
            return (ticker in tickers) == now_present

        lo, hi = 0, len(revs) - 1
        if not ok(hi):
            return None
        while lo < hi:
            mid = (lo + hi) // 2
            if ok(mid):
                hi = mid
            else:
                lo = mid + 1
        return dt.date.fromisoformat(revs[lo][1][:10])
