"""Incremental EDGAR fundamentals via the daily filing index.

``https://www.sec.gov/Archives/edgar/daily-index/YYYY/QTRq/master.YYYYMMDD.idx``
lists every filing accepted that day (``CIK|Company|Form|Date|File``). One
request per business day tells us exactly which security-master CIKs filed
a periodic report, so the nightly job re-pulls companyfacts only for those —
instead of either never refreshing (the old placeholder) or re-pulling ~500
large JSONs every night.

State (the last fully processed date) lives in
``data/raw/edgar/daily_index_state.json``.
"""

from __future__ import annotations

import datetime as dt
import json
import logging

import httpx
import pandas as pd

from ..config import settings

log = logging.getLogger(__name__)

FORMS = {"10-K", "10-K/A", "10-Q", "10-Q/A", "20-F", "20-F/A", "40-F", "40-F/A", "10-KT", "6-K"}
URL = "https://www.sec.gov/Archives/edgar/daily-index/{y}/QTR{q}/master.{d}.idx"
MAX_CATCHUP_DAYS = 120


def _state_path():
    return settings.raw_dir / "edgar" / "daily_index_state.json"


def load_last_processed(default: dt.date) -> dt.date:
    p = _state_path()
    if p.exists():
        return dt.date.fromisoformat(json.loads(p.read_text())["last_processed"])
    return default


def save_last_processed(d: dt.date) -> None:
    p = _state_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"last_processed": d.isoformat()}))


def filers_on(day: dt.date, client: httpx.Client) -> pd.DataFrame:
    """(cik, form) of periodic filings accepted on *day*; empty on non-business days."""
    q = (day.month - 1) // 3 + 1
    r = client.get(URL.format(y=day.year, q=q, d=day.strftime("%Y%m%d")))
    if r.status_code in (403, 404):  # weekends/holidays have no index file
        return pd.DataFrame(columns=["cik", "form"])
    r.raise_for_status()
    rows = []
    for line in r.text.splitlines():
        parts = line.split("|")
        if len(parts) == 5 and parts[0].isdigit() and parts[2] in FORMS:
            rows.append((parts[0].zfill(10), parts[2]))
    return pd.DataFrame(rows, columns=["cik", "form"])


def ciks_with_new_filings(since: dt.date, until: dt.date) -> set[str]:
    """Union of CIKs with a periodic filing in (since, until]."""
    headers = {"User-Agent": settings.require_sec_user_agent(), "Accept-Encoding": "gzip, deflate"}
    out: set[str] = set()
    with httpx.Client(headers=headers, timeout=60.0) as client:
        d = since + dt.timedelta(days=1)
        while d <= until:
            if d.weekday() < 5:
                out |= set(filers_on(d, client)["cik"])
            d += dt.timedelta(days=1)
    return out


def refresh_changed(sm: pd.DataFrame, *, today: dt.date | None = None) -> tuple[int, dt.date, dt.date]:
    """Re-pull companyfacts for security-master CIKs that filed since the last run.

    Returns (n_ciks_refreshed, window_start, window_end).
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    from ..store import writer as w
    from .companyfacts_api import fetch_companyfacts, normalize_facts

    today = today or dt.date.today()
    until = today - dt.timedelta(days=1)  # today's index is still being written
    since = load_last_processed(default=until - dt.timedelta(days=7))
    since = max(since, until - dt.timedelta(days=MAX_CATCHUP_DAYS))
    if since >= until:
        return 0, since, until
    ours = set(sm["cik"].dropna())
    changed = sorted(ciks_with_new_filings(since, until) & ours)
    tables = []
    for cik in changed:
        try:
            raw = fetch_companyfacts(cik, refresh=True)
            if raw:
                t = normalize_facts(cik, raw)
                if t.num_rows:
                    tables.append(t)
        except Exception as e:  # keep going; the next run retries via the window
            log.warning("companyfacts refresh failed for %s: %s", cik, e)
    if tables:
        combined = pa.concat_tables(tables)
        combined = combined.append_column("filed_year", pc.year(pc.cast(combined["filed"], pa.timestamp("us"))))
        w.append_to_partition(
            combined,
            w.fundamentals_facts_dir(),
            partition_col="filed_year",
            primary_key=["cik", "concept", "period_start", "period_end", "accession_number"],
        )
    save_last_processed(until)
    return len(changed), since, until
