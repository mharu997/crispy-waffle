"""Tiingo News REST client.

Endpoint: https://api.tiingo.com/tiingo/news
Docs: https://www.tiingo.com/documentation/news

Tiingo's News API returns articles tagged with tickers and topics, plus
two timestamps that matter:

* ``publishedDate`` — what the source claims about when the article ran.
* ``crawlDate`` — when Tiingo's crawler first indexed it. Tiingo backfills
  historical articles when adding new sources; for those, ``crawlDate``
  can be much later than ``publishedDate``. Backtests must filter on
  ``crawlDate`` to stay PIT-safe.

This module is a thin HTTP wrapper. Parsing into Arrow happens in
``loader.py`` so the client stays small and testable.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Literal

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ..config import settings
from ..prices import symbols

log = logging.getLogger(__name__)

BASE_URL = "https://api.tiingo.com/tiingo/news"

SortBy = Literal["crawlDate", "publishedDate"]


def _headers() -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Token {settings.require_tiingo()}",
    }


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type(httpx.HTTPError),
    reraise=True,
)
def fetch_news(
    *,
    tickers: list[str] | None = None,
    tags: list[str] | None = None,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
    sort_by: SortBy = "crawlDate",
    limit: int = 1000,
    offset: int = 0,
    only_with_tickers: bool = True,
) -> list[dict]:
    """Return one page of Tiingo news articles as raw dicts.

    Parameters mirror the REST API. Pagination is the caller's job: bump
    ``offset`` by ``limit`` until a short page is returned. ``sort_by``
    defaults to ``crawlDate`` ascending so that paginating-then-checkpointing
    by max-seen ``crawlDate`` is straightforward and resumable.

    Returns ``[]`` for 404 or empty responses.
    """
    params: dict[str, str | int] = {
        "limit": limit,
        "offset": offset,
        "sortBy": sort_by,
    }
    if tickers:
        # Tiingo expects dash-form for class shares (BRK-B, not BRK.B).
        # Reuse the prices-side normalizer so the rule lives in one place.
        params["tickers"] = ",".join(symbols.for_tiingo(t).lower() for t in tickers)
    if tags:
        params["tags"] = ",".join(tags)
    if start_date is not None:
        params["startDate"] = start_date.isoformat()
    if end_date is not None:
        params["endDate"] = end_date.isoformat()
    if only_with_tickers:
        params["onlyWithTickers"] = "true"

    r = httpx.get(BASE_URL, headers=_headers(), params=params, timeout=30.0)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        log.warning("Unexpected Tiingo news payload (not a list): %r", data)
        return []
    return data
