"""SIC code extraction from SEC EDGAR (PIT-rigorous sector classification).

Every EDGAR-registered company has an SIC (Standard Industrial Classification)
code in its submission metadata. SIC predates GICS but is fully PIT — it's
recorded with every filing and rarely changes.

We pull the *current* SIC from ``data.sec.gov/submissions/CIK##########.json``
and store it as our canonical historical sector for the company. For
companies whose SIC has changed historically, the per-filing SIC (in the
filing header) gives the PIT-correct value at filing time; we don't yet
parse that, so the assumption is "SIC is roughly stable over our window."

References:
    - SEC submissions endpoint: https://data.sec.gov/submissions/
    - SIC code list: https://www.sec.gov/search-filings/standard-industrial-classification-sic-code-list
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ..config import settings

log = logging.getLogger(__name__)

SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type(httpx.HTTPError),
    reraise=True,
)
def _fetch_submissions(cik: str) -> dict | None:
    """Download the submissions JSON for one CIK; None on 404."""
    cik_clean = cik.lstrip("0").zfill(10)
    url = SUBMISSIONS_URL.format(cik=cik_clean)
    headers = {"User-Agent": settings.require_sec_user_agent(), "Accept-Encoding": "gzip, deflate"}
    r = httpx.get(url, headers=headers, timeout=30.0)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def _cache_path(cik: str) -> Path:
    return settings.raw_dir / "edgar" / "submissions" / f"CIK{cik.zfill(10)}.json"


def fetch_submissions(cik: str, *, refresh: bool = False) -> dict | None:
    """Fetch submissions JSON for a CIK, caching to disk."""
    cik_clean = cik.lstrip("0").zfill(10)
    cache = _cache_path(cik_clean)
    if not refresh and cache.exists():
        return json.loads(cache.read_text())

    data = _fetch_submissions(cik_clean)
    if data is None:
        return None
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(data))
    return data


def get_sic_for_cik(cik: str) -> tuple[int | None, str | None]:
    """Return (sic_code, sic_description) for *cik*, or (None, None) if unknown."""
    data = fetch_submissions(cik)
    if not data:
        return None, None
    try:
        sic = int(data.get("sic"))
    except (TypeError, ValueError):
        sic = None
    return sic, data.get("sicDescription")
