"""SEC EDGAR ``companyfacts`` JSON loader (per-CIK, incremental).

EDGAR exposes a free public endpoint that returns every XBRL-tagged fact
ever filed for a company:

    https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json

Each fact in the response carries:

* ``accn`` — the SEC accession number of the filing that produced it (the
  primary key component that distinguishes vintages of the same fact).
* ``filed`` — the date the filing was submitted (the PIT "knowable on" cutoff).
* ``form`` — the form type ("10-K", "10-Q", "10-K/A", etc.).
* ``fy``, ``fp`` — fiscal year and period ("Q1"|"Q2"|"Q3"|"FY").
* ``frame`` — XBRL frame identifier (e.g. "CY2018Q3I"), useful for
  cross-company ratio analysis.
* ``start``, ``end`` — period start/end dates.
* ``val`` — the numeric value.
* ``unit`` — the XBRL unit (e.g. "USD", "shares").

This loader fetches one CIK at a time and produces an Arrow Table
conforming to ``schemas.FUNDAMENTALS_FACTS``. The append-only writer
materializes them into ``fundamentals_facts/year={filed_year}/``.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import time
from pathlib import Path

import httpx
import pandas as pd
import pyarrow as pa
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ..config import settings
from ..identity import sec_headers
from ..store import schemas

log = logging.getLogger(__name__)

COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# SEC enforces 10 req/sec. We add a small floor delay between requests to
# avoid hitting the limit during bulk loads.
RATE_LIMIT_DELAY_SEC = 0.12  # ~8 requests/sec, well under the limit


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type(httpx.HTTPError),
    reraise=True,
)
def _fetch_raw(cik: str) -> dict | None:
    """Download one CIK's companyfacts JSON; return None on 404."""
    cik_clean = cik.lstrip("0").zfill(10)
    url = COMPANYFACTS_URL.format(cik=cik_clean)
    headers = {"User-Agent": settings.require_sec_user_agent(), "Accept-Encoding": "gzip, deflate"}
    r = httpx.get(url, headers=headers, timeout=30.0)
    if r.status_code == 404:
        log.info("EDGAR companyfacts 404 for CIK %s (no XBRL filings)", cik_clean)
        return None
    r.raise_for_status()
    return r.json()


def _cache_path(cik: str) -> Path:
    return settings.raw_dir / "edgar" / "companyfacts" / f"CIK{cik.zfill(10)}.json"


def fetch_companyfacts(cik: str, *, refresh: bool = False) -> dict | None:
    """Fetch a single CIK's companyfacts, caching the raw JSON to disk."""
    cik_clean = cik.lstrip("0").zfill(10)
    cache = _cache_path(cik_clean)
    if not refresh and cache.exists():
        return json.loads(cache.read_text())

    data = _fetch_raw(cik_clean)
    if data is None:
        return None
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(data))
    return data


def normalize_facts(cik: str, raw: dict) -> pa.Table:
    """Flatten a companyfacts JSON into a wide rows table.

    The raw JSON is shaped::

        {
          "cik": int,
          "entityName": str,
          "facts": {
            "us-gaap": {
              "Revenues": {
                "label": "...", "description": "...",
                "units": {
                  "USD": [
                    {"start": "...", "end": "...", "val": ...,
                     "accn": "...", "fy": ..., "fp": "...",
                     "form": "...", "filed": "...", "frame": "..."},
                    ...
                  ]
                }
              },
              ...
            },
            "dei": {...}
          }
        }
    """
    cik_padded = str(cik).zfill(10)
    today = dt.date.today()
    rows: list[dict] = []
    facts_root = raw.get("facts", {})
    for taxonomy, concepts in facts_root.items():
        for concept_name, concept_data in concepts.items():
            qualified = f"{taxonomy}:{concept_name}"
            for unit, observations in concept_data.get("units", {}).items():
                for obs in observations:
                    rows.append(
                        {
                            "cik": cik_padded,
                            "concept": qualified,
                            "unit": unit,
                            "period_start": _parse_date(obs.get("start")),
                            "period_end": _parse_date(obs.get("end")),
                            "val": float(obs["val"]),
                            "accession_number": obs.get("accn", ""),
                            "filed": _parse_date(obs.get("filed")) or today,
                            "form": obs.get("form", ""),
                            "fy": obs.get("fy"),
                            "fp": obs.get("fp"),
                            "frame": obs.get("frame"),
                            "ingestion_date": today,
                        }
                    )
    if not rows:
        empty = pd.DataFrame(
            columns=[f.name for f in schemas.FUNDAMENTALS_FACTS]
        )
        return pa.Table.from_pandas(
            empty, schema=schemas.FUNDAMENTALS_FACTS, preserve_index=False
        )
    df = pd.DataFrame(rows)
    df["fy"] = pd.to_numeric(df["fy"], errors="coerce").astype("Int32")
    return pa.Table.from_pandas(
        df, schema=schemas.FUNDAMENTALS_FACTS, preserve_index=False
    )


def _parse_date(s: str | None) -> dt.date | None:
    if not s:
        return None
    try:
        return dt.date.fromisoformat(s)
    except (TypeError, ValueError):
        return None


def fetch_universe_facts(
    ciks: list[str],
    *,
    refresh: bool = False,
) -> pa.Table:
    """Fetch + normalize companyfacts for a list of CIKs; return one combined Table."""
    tables: list[pa.Table] = []
    for i, cik in enumerate(ciks, 1):
        if i % 50 == 0:
            log.info("Fundamentals fetch progress: %s/%s", i, len(ciks))
        try:
            raw = fetch_companyfacts(cik, refresh=refresh)
        except Exception as e:
            log.warning("Companyfacts failed for CIK %s: %s", cik, e)
            continue
        if raw is None:
            continue
        try:
            t = normalize_facts(cik, raw)
        except Exception as e:
            log.warning("Normalize failed for CIK %s: %s", cik, e)
            continue
        if t.num_rows > 0:
            tables.append(t)
        time.sleep(RATE_LIMIT_DELAY_SEC)

    if not tables:
        return pa.Table.from_pandas(
            pd.DataFrame(columns=[f.name for f in schemas.FUNDAMENTALS_FACTS]),
            schema=schemas.FUNDAMENTALS_FACTS,
            preserve_index=False,
        )
    return pa.concat_tables(tables)
