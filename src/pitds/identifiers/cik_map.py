"""Ticker → CIK mapping from SEC's free public JSON files.

EDGAR publishes two JSONs covering the currently-active universe:

* https://www.sec.gov/files/company_tickers.json
  — flat list of {cik, ticker, title}.
* https://www.sec.gov/files/company_tickers_exchange.json
  — same with exchange info (NYSE, NASDAQ, etc.).

These cover **currently-active** tickers only. Delisted tickers get NULL
CIK at this stage; Phase B's OpenFIGI integration fills the gaps.

The ``SEC_USER_AGENT`` env var must be set; SEC enforces a rate limit of
10 requests/second per IP and rejects requests without a User-Agent
identifying the requester.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path

import httpx

from ..config import settings
from ..identity import sec_headers

log = logging.getLogger(__name__)

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
TICKERS_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"

REQUEST_TIMEOUT = 30.0


def _cache_file() -> Path:
    return settings.raw_dir / "edgar" / "company_tickers.json"


def _exchange_cache_file() -> Path:
    return settings.raw_dir / "edgar" / "company_tickers_exchange.json"


def fetch_ticker_jsons(*, refresh: bool = False) -> tuple[Path, Path]:
    """Download SEC's currently-active ticker JSONs to local cache."""
    headers = {
        "User-Agent": settings.require_sec_user_agent(),
        "Accept-Encoding": "gzip, deflate",
    }
    cache, exch_cache = _cache_file(), _exchange_cache_file()
    cache.parent.mkdir(parents=True, exist_ok=True)

    if refresh or not cache.exists():
        log.info("Downloading %s", TICKERS_URL)
        r = httpx.get(TICKERS_URL, timeout=REQUEST_TIMEOUT, headers=headers)
        r.raise_for_status()
        cache.write_bytes(r.content)

    if refresh or not exch_cache.exists():
        log.info("Downloading %s", TICKERS_EXCHANGE_URL)
        r = httpx.get(TICKERS_EXCHANGE_URL, timeout=REQUEST_TIMEOUT, headers=headers)
        r.raise_for_status()
        exch_cache.write_bytes(r.content)

    return cache, exch_cache


@lru_cache(maxsize=1)
def load_ticker_to_cik() -> dict[str, str]:
    """Return a {ticker: zero-padded-cik} dict for currently-active US filers."""
    import json

    cache, _ = fetch_ticker_jsons()
    raw = json.loads(cache.read_text())

    # company_tickers.json format: {"0": {"cik_str": 320193, "ticker": "AAPL", ...}, ...}
    mapping: dict[str, str] = {}
    for entry in raw.values():
        ticker = str(entry.get("ticker", "")).strip().upper()
        cik = entry.get("cik_str")
        if ticker and cik is not None:
            mapping[ticker] = str(cik).zfill(10)
    log.info("Loaded %s ticker→CIK mappings from EDGAR", len(mapping))
    return mapping


def resolve_ciks(
    tickers: list[str],
    *,
    seed: dict[str, str] | None = None,
) -> dict[str, str | None]:
    """Resolve a list of tickers to CIKs using the SEC JSON map.

    ``seed`` is a higher-priority lookup (typically fja05680's current
    snapshot) consulted first. Tickers that resolve to None are unknown to
    SEC's *currently-active* feed — typically delisted and require OpenFIGI
    or EDGAR filer search to resolve.
    """
    sec_map = load_ticker_to_cik()
    seed = seed or {}
    out: dict[str, str | None] = {}
    for t in tickers:
        tu = t.strip().upper()
        if tu in seed:
            out[t] = seed[tu]
        elif tu in sec_map:
            out[t] = sec_map[tu]
        else:
            # Try common normalization: BRK.B → BRK-B (SEC uses dash, fja uses dot)
            alt = tu.replace(".", "-")
            if alt in sec_map:
                out[t] = sec_map[alt]
            else:
                alt2 = tu.replace("-", ".")
                if alt2 in sec_map:
                    out[t] = sec_map[alt2]
                else:
                    out[t] = None
    return out
