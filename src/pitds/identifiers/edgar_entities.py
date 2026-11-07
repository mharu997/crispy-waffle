"""EDGAR entity evidence: filing spans and the full name → CIK index.

Two questions the security master needs EDGAR to answer:

1. *Was CIK X an operating SEC filer during [a, b)?* — answered by the dates
   of its periodic reports (10-K/10-Q/20-F/40-F). A holding-company
   reorganization (Google Inc → Alphabet 2015, Eaton Corp → Eaton plc 2012)
   creates a new CIK whose periodic filings start at the reorg; any earlier
   period belongs to the predecessor CIK.
2. *Which CIKs have ever carried name N?* — answered by
   ``cik-lookup-data.txt``, SEC's index of every entity name (current and
   former) with its CIK.

Both are cached under ``data/raw/edgar/``. SEC fair-access: ≤10 req/s with a
declared User-Agent; we pace at ~8 req/s.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import re
import threading
import time
from functools import lru_cache

import httpx
import pandas as pd

from ..config import settings
from ..sectors.sic_edgar import fetch_submissions

log = logging.getLogger(__name__)

PERIODIC_FORMS = {
    "10-K", "10-K405", "10-KT", "10-KSB", "10-Q", "10-QSB",
    "20-F", "40-F", "10-K/A", "10-Q/A", "20-F/A", "40-F/A",
}
LOOKUP_URL = "https://www.sec.gov/Archives/edgar/cik-lookup-data.txt"
PAGE_URL = "https://data.sec.gov/submissions/{name}"

_lock = threading.Lock()
_last = [0.0]


def _pace(min_interval: float = 0.125) -> None:
    with _lock:
        wait = _last[0] + min_interval - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last[0] = time.monotonic()


def _headers() -> dict[str, str]:
    return {"User-Agent": settings.require_sec_user_agent(), "Accept-Encoding": "gzip, deflate"}


def _page(name: str) -> dict:
    path = settings.raw_dir / "edgar" / "submissions" / "pages" / name
    if path.exists():
        return json.loads(path.read_text())
    _pace()
    r = httpx.get(PAGE_URL.format(name=name), headers=_headers(), timeout=60.0)
    r.raise_for_status()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(r.text)
    return r.json()


@lru_cache(maxsize=4096)
def filings(cik: str) -> pd.DataFrame:
    """All filings (form, filing_date) for *cik*, across paginated history."""
    cik = str(cik).zfill(10)
    _pace()
    sub = fetch_submissions(cik)
    if not sub:
        return pd.DataFrame(columns=["form", "filing_date"])
    blocks = [sub["filings"]["recent"]]
    for f in sub["filings"].get("files", []):
        try:
            blocks.append(_page(f["name"]))
        except Exception as e:
            log.warning("EDGAR page %s failed: %s", f["name"], e)
    frames = [
        pd.DataFrame({"form": b.get("form", []), "filing_date": b.get("filingDate", [])})
        for b in blocks
    ]
    df = pd.concat(frames, ignore_index=True)
    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce").dt.date
    return df.dropna()


@lru_cache(maxsize=4096)
def periodic_span(cik: str) -> tuple[dt.date | None, dt.date | None]:
    """(first, last) periodic-report filing date for *cik*."""
    f = filings(cik)
    p = f.loc[f["form"].isin(PERIODIC_FORMS), "filing_date"]
    if p.empty:
        return None, None
    return p.min(), p.max()


def submissions_asof(cik: str) -> dt.date:
    """Date the cached submissions JSON for *cik* was fetched (file mtime)."""
    path = settings.raw_dir / "edgar" / "submissions" / f"CIK{str(cik).zfill(10)}.json"
    if path.exists():
        return dt.date.fromtimestamp(path.stat().st_mtime)
    return dt.date.today()


@lru_cache(maxsize=4096)
def entity_names(cik: str) -> list[str]:
    """Current + former EDGAR names for *cik*."""
    sub = fetch_submissions(str(cik).zfill(10)) or {}
    names = [sub.get("name") or ""]
    names += [n.get("name", "") for n in sub.get("formerNames", [])]
    return [n for n in names if n]


def covers(cik: str, start: dt.date, end: dt.date | None, *, slack_days: int = 200) -> bool:
    """True if *cik* filed periodic reports spanning ``[start, end)`` (± slack)."""
    first, last = periodic_span(cik)
    if first is None:
        return False
    end = end or dt.date.today()
    slack = dt.timedelta(days=slack_days)
    return first <= start + slack and last >= min(end, dt.date.today()) - slack


# ---------------------------------------------------------------------------
# Name index
# ---------------------------------------------------------------------------

_SUFFIX = re.compile(
    r"\b(the|inc|incorporated|corp|corporation|co|company|companies|ltd|limited|plc|"
    r"n\.?v|s\.?a|llc|lp|l\.p|holdings?|group|cos|de|new|class [a-c]|/[a-z]{2}/?)\b"
)


def normalize_name(name: str) -> str:
    s = name.lower().replace("&", " and ")
    s = re.sub(r"\(.*?\)", " ", s)
    s = re.sub(r"[^a-z0-9/ ]", " ", s)
    s = _SUFFIX.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


@lru_cache(maxsize=1)
def name_index() -> pd.DataFrame:
    """DataFrame(name, cik, norm) of every EDGAR entity name ever registered."""
    path = settings.raw_dir / "edgar" / "cik-lookup-data.txt"
    if not path.exists():
        log.info("Downloading %s", LOOKUP_URL)
        with httpx.stream("GET", LOOKUP_URL, headers=_headers(), timeout=600.0) as r:
            r.raise_for_status()
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "wb") as fh:
                for chunk in r.iter_bytes():
                    fh.write(chunk)
    rows = []
    with open(path, encoding="latin-1") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line.endswith(":"):
                continue
            name, _, rest = line[:-1].rpartition(":")
            if rest.isdigit():
                rows.append((name, rest.zfill(10)))
    df = pd.DataFrame(rows, columns=["name", "cik"])
    df["norm"] = df["name"].map(normalize_name)
    return df


def search_name(name: str, *, limit: int = 8, min_score: int = 88) -> list[tuple[str, str, float]]:
    """Fuzzy-search EDGAR names; returns [(cik, edgar_name, score)] best first."""
    from rapidfuzz import fuzz, process

    idx = name_index()
    q = normalize_name(name)
    if not q:
        return []
    # Pre-filter on the first token to keep rapidfuzz fast over ~1M names.
    first = q.split()[0]
    sub = idx.loc[idx["norm"].str.startswith(first)]
    if sub.empty:
        sub = idx
    hits = process.extract(q, sub["norm"].tolist(), scorer=fuzz.token_sort_ratio, limit=limit * 4)
    out, seen = [], set()
    for _, score, i in hits:
        if score < min_score:
            continue
        row = sub.iloc[i]
        if row["cik"] in seen:
            continue
        seen.add(row["cik"])
        out.append((row["cik"], row["name"], float(score)))
        if len(out) >= limit:
            break
    return out
