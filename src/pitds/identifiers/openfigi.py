"""OpenFIGI integration for historical/delisted ticker resolution.

OpenFIGI (https://www.openfigi.com/api) is the canonical free service for
mapping CUSIP/ISIN/ticker → FIGI (Bloomberg's Financial Instrument Global
Identifier). Critically, it works for delisted tickers, which SEC's
``company_tickers.json`` does not.

This module is a Phase B placeholder; Phase A relies on SEC's currently-
active feed plus fja05680's current snapshot for ~80% coverage.

Rate limits (no key): 25 requests/min, 10 mappings/request → ~250 mappings/min.
With a free key (https://www.openfigi.com/api): 250 requests/min, 100
mappings/request → ~25,000 mappings/min.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from ..config import settings

log = logging.getLogger(__name__)

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"


def map_tickers(
    tickers: list[str],
    *,
    exchange_code: str = "US",
) -> list[dict[str, Any]]:
    """Map a list of tickers to FIGI; returns one result dict per input.

    Each result has keys: figi, name, ticker, exchCode, compositeFIGI,
    securityType, marketSector, etc. Unmappable tickers return ``{"warning": ...}``.

    Phase A does not use this function; it's wired in Phase B.
    """
    headers = {"Content-Type": "application/json"}
    if settings.openfigi_api_key:
        headers["X-OPENFIGI-APIKEY"] = settings.openfigi_api_key

    payload = [
        {"idType": "TICKER", "idValue": t, "exchCode": exchange_code}
        for t in tickers
    ]
    r = httpx.post(OPENFIGI_URL, json=payload, headers=headers, timeout=30.0)
    r.raise_for_status()
    return r.json()
