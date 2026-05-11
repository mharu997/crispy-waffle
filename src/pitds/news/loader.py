"""News backfill orchestrator.

For each ticker batch, paginate Tiingo's news endpoint by ``offset``
until a short page is returned, then move to the next batch. Articles
that mention multiple universe tickers may be returned more than once
across batches; ``append_to_partition`` dedupes them on ``article_id``
when writing.

PIT contract: every row carries ``crawl_date`` (Tiingo first-seen) and
``ingestion_date`` (when *we* pulled it). The partition layout keys on
``crawl_year`` so the dominant ``WHERE crawl_date <= as_of`` query
prunes whole partitions.
"""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Iterable
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from ..store import schemas, writer
from . import tiingo

log = logging.getLogger(__name__)


# Pagination safety cap. Tiingo's news archive on the Power tier appears
# to be a rolling ~6-month window per ticker, so a single batch realistically
# returns under 60K articles. This cap guards against a runaway pull if
# the API ever streams duplicates indefinitely.
MAX_PAGES_PER_BATCH = 200

# Threshold (in days) for warning when the actual returned crawl_date range
# diverges from what the caller requested. Tiingo's startDate/endDate query
# params don't reliably enforce the upper bound when sortBy=crawlDate, and
# old data may be silently inaccessible if the user's tier window is
# shorter than the requested span.
DATE_DRIFT_WARN_DAYS = 7


def _chunked(seq: list[str], n: int) -> Iterable[list[str]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _parse_ts(s: str) -> dt.datetime:
    """Parse Tiingo ISO-8601 timestamp into a UTC-aware datetime."""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    d = dt.datetime.fromisoformat(s)
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(dt.timezone.utc)


def _normalize_article(item: dict, ingestion_date: dt.date) -> dict | None:
    """Convert one Tiingo article into a row dict matching the NEWS schema.

    Returns None if the article is missing required fields; the caller logs
    and skips. We accept that some Tiingo records may be malformed rather
    than failing a whole batch.
    """
    try:
        article_id = int(item["id"])
        url = str(item["url"])
        published = _parse_ts(item["publishedDate"])
        crawled = _parse_ts(item["crawlDate"])
    except (KeyError, ValueError, TypeError) as e:
        log.warning("Skipping malformed article %s: %s", item.get("id", "?"), e)
        return None

    return {
        "article_id": article_id,
        "title": str(item.get("title") or ""),
        "description": item.get("description"),
        "url": url,
        "source": str(item.get("source") or ""),
        "published_date": published,
        "crawl_date": crawled,
        "tickers": [str(t).upper() for t in (item.get("tickers") or [])],
        "tags": [str(t).lower() for t in (item.get("tags") or [])],
        "ingested_via": "tiingo_news",
        "ingestion_date": ingestion_date,
    }


def fetch_universe_news(
    *,
    tickers: list[str],
    start: dt.date,
    end: dt.date,
    batch_size: int = 5,
    page_limit: int = 1000,
) -> pa.Table:
    """Pull news for *tickers* in [start, end], return a NEWS-schema Arrow Table.

    Tickers are batched ``batch_size`` per request (Tiingo accepts a
    comma-separated list). Within each batch we paginate by ``offset``
    until a page shorter than ``page_limit`` is returned.

    Per-batch failures are logged and the loop continues — one bad batch
    does not abort the whole backfill.
    """
    today = dt.date.today()
    rows: list[dict] = []
    batches = list(_chunked(tickers, batch_size))
    seen_ids: set[int] = set()
    actual_min_crawl: dt.datetime | None = None
    actual_max_crawl: dt.datetime | None = None

    for batch_idx, batch in enumerate(batches, 1):
        offset = 0
        batch_added = 0
        batch_min: dt.datetime | None = None
        batch_max: dt.datetime | None = None
        for page_idx in range(MAX_PAGES_PER_BATCH):
            try:
                page = tiingo.fetch_news(
                    tickers=batch,
                    start_date=start,
                    end_date=end,
                    sort_by="crawlDate",
                    limit=page_limit,
                    offset=offset,
                )
            except Exception as e:
                log.warning(
                    "News fetch failed for batch %s offset %d: %s",
                    batch, offset, e,
                )
                break
            if not page:
                break
            for item in page:
                normalized = _normalize_article(item, today)
                if normalized is None:
                    continue
                if normalized["article_id"] in seen_ids:
                    continue
                seen_ids.add(normalized["article_id"])
                rows.append(normalized)
                batch_added += 1
                cd = normalized["crawl_date"]
                if batch_min is None or cd < batch_min:
                    batch_min = cd
                if batch_max is None or cd > batch_max:
                    batch_max = cd
            if len(page) < page_limit:
                break
            offset += page_limit
        else:
            log.warning(
                "News batch %s hit pagination safety cap (%d pages × %d/page); "
                "additional articles may exist. Consider narrowing the window.",
                batch, MAX_PAGES_PER_BATCH, page_limit,
            )

        if batch_min is not None:
            log.info(
                "News batch %d/%d (%s): %d new articles, crawl_date %s → %s",
                batch_idx, len(batches), ",".join(batch), batch_added,
                batch_min.date(), batch_max.date(),
            )
            if actual_min_crawl is None or batch_min < actual_min_crawl:
                actual_min_crawl = batch_min
            if actual_max_crawl is None or batch_max > actual_max_crawl:
                actual_max_crawl = batch_max
        else:
            log.info(
                "News batch %d/%d (%s): 0 new articles",
                batch_idx, len(batches), ",".join(batch),
            )

    _warn_on_date_drift(start, end, actual_min_crawl, actual_max_crawl)

    if not rows:
        return pa.Table.from_pylist([], schema=schemas.NEWS)
    return pa.Table.from_pylist(rows, schema=schemas.NEWS)


def _warn_on_date_drift(
    requested_start: dt.date,
    requested_end: dt.date,
    actual_min_crawl: dt.datetime | None,
    actual_max_crawl: dt.datetime | None,
) -> None:
    """Compare requested date window to what Tiingo actually returned.

    Tiingo's startDate/endDate params don't reliably enforce the upper bound
    when sorting by crawlDate, and a tier with limited history (e.g., Power
    is ~6 months) silently returns the most-recent N months when older data
    is requested. This routine surfaces that drift so the user isn't misled
    by their CLI flags.
    """
    if actual_min_crawl is None:
        log.warning(
            "No articles returned for the requested window %s → %s. "
            "If you expected results, check tier history depth and ticker spelling.",
            requested_start, requested_end,
        )
        return

    actual_min = actual_min_crawl.date()
    actual_max = actual_max_crawl.date() if actual_max_crawl else actual_min
    log.info(
        "News window: requested %s → %s, actual crawl_date %s → %s",
        requested_start, requested_end, actual_min, actual_max,
    )

    start_drift = (actual_min - requested_start).days
    end_drift = (requested_end - actual_max).days
    if start_drift > DATE_DRIFT_WARN_DAYS:
        log.warning(
            "Earliest returned article (crawl_date=%s) is %d days *after* "
            "requested start (%s). Older articles likely outside your Tiingo "
            "tier's history window.",
            actual_min, start_drift, requested_start,
        )
    if end_drift > DATE_DRIFT_WARN_DAYS:
        log.warning(
            "Latest returned article (crawl_date=%s) is %d days *before* "
            "requested end (%s). The Tiingo query may be silently truncating "
            "the upper date bound.",
            actual_max, end_drift, requested_end,
        )


def write_news(table: pa.Table) -> list[Path]:
    """Append a NEWS-schema *table* into the hive-partitioned news dataset.

    Adds the ``crawl_year`` partition column derived from ``crawl_date``,
    then delegates to ``writer.append_to_partition``. Within each year
    partition, rows are deduped on ``article_id`` keeping the latest
    ``ingestion_date``.
    """
    if table.num_rows == 0:
        log.info("No news rows to write.")
        return []
    crawl_year = pc.year(table["crawl_date"]).cast(pa.int32())
    partitioned = table.append_column("crawl_year", crawl_year)
    return writer.append_to_partition(
        partitioned,
        writer.news_dir(),
        partition_col="crawl_year",
        primary_key=["article_id"],
    )


def backfill(
    tickers: Iterable[str],
    *,
    start: dt.date,
    end: dt.date,
    batch_size: int = 5,
) -> int:
    """Fetch and persist news for *tickers* over [start, end]; return rowcount written."""
    table = fetch_universe_news(
        tickers=list(tickers),
        start=start,
        end=end,
        batch_size=batch_size,
    )
    if table.num_rows == 0:
        log.warning("No news articles fetched for %d tickers.", len(list(tickers)))
        return 0
    write_news(table)
    return table.num_rows
