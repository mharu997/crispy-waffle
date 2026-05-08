"""Unit tests for the news ingester.

Network access is mocked at the ``tiingo.fetch_news`` boundary, so these
tests run offline and don't require a TIINGO_API_KEY.
"""

from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pitds.news import loader, tiingo as tiingo_client
from pitds.store import schemas


def _sample_article(article_id: int = 1, **overrides) -> dict:
    """Tiingo-shaped article dict for fixtures."""
    base = {
        "id": article_id,
        "title": "Apple beats earnings expectations",
        "description": "Apple Inc. reported strong quarterly results.",
        "url": f"https://example.com/article/{article_id}",
        "source": "example.com",
        "publishedDate": "2025-04-30T20:00:00Z",
        "crawlDate": "2025-04-30T20:05:13Z",
        "tickers": ["aapl", "msft"],
        "tags": ["earnings", "tech"],
    }
    base.update(overrides)
    return base


# ─────────────────────────────────────────────────────────────────────────────
# Timestamp parsing
# ─────────────────────────────────────────────────────────────────────────────


def test_parse_ts_z_suffix():
    d = loader._parse_ts("2025-04-30T20:00:00Z")
    assert d == dt.datetime(2025, 4, 30, 20, 0, 0, tzinfo=dt.timezone.utc)


def test_parse_ts_offset_suffix():
    d = loader._parse_ts("2025-04-30T16:00:00-04:00")
    assert d == dt.datetime(2025, 4, 30, 20, 0, 0, tzinfo=dt.timezone.utc)


def test_parse_ts_microseconds():
    d = loader._parse_ts("2025-04-30T20:05:13.123456Z")
    assert d.microsecond == 123456
    assert d.tzinfo is dt.timezone.utc


# ─────────────────────────────────────────────────────────────────────────────
# Article normalization
# ─────────────────────────────────────────────────────────────────────────────


def test_normalize_article_happy_path():
    item = _sample_article(article_id=42)
    row = loader._normalize_article(item, dt.date(2025, 5, 1))
    assert row is not None
    assert row["article_id"] == 42
    assert row["title"] == "Apple beats earnings expectations"
    assert row["source"] == "example.com"
    assert row["tickers"] == ["AAPL", "MSFT"]  # uppercased
    assert row["tags"] == ["earnings", "tech"]  # lowercased
    assert row["ingested_via"] == "tiingo_news"
    assert row["ingestion_date"] == dt.date(2025, 5, 1)
    assert row["crawl_date"].tzinfo is dt.timezone.utc


def test_normalize_article_missing_required_returns_none():
    bad = _sample_article()
    del bad["publishedDate"]
    row = loader._normalize_article(bad, dt.date(2025, 5, 1))
    assert row is None


def test_normalize_article_handles_null_description():
    item = _sample_article(description=None)
    row = loader._normalize_article(item, dt.date(2025, 5, 1))
    assert row is not None
    assert row["description"] is None


def test_normalize_article_handles_empty_lists():
    item = _sample_article(tickers=None, tags=None)
    row = loader._normalize_article(item, dt.date(2025, 5, 1))
    assert row is not None
    assert row["tickers"] == []
    assert row["tags"] == []


# ─────────────────────────────────────────────────────────────────────────────
# fetch_universe_news with mocked HTTP
# ─────────────────────────────────────────────────────────────────────────────


def test_fetch_universe_news_returns_arrow_table_matching_schema(monkeypatch):
    """One short page → one batch fetched, table conforms to NEWS schema."""
    articles = [_sample_article(i) for i in range(3)]
    calls: list[dict] = []

    def fake_fetch_news(**kwargs):
        calls.append(kwargs)
        return articles if kwargs["offset"] == 0 else []

    monkeypatch.setattr(loader.tiingo, "fetch_news", fake_fetch_news)

    table = loader.fetch_universe_news(
        tickers=["AAPL", "MSFT", "GOOGL"],
        start=dt.date(2025, 4, 1),
        end=dt.date(2025, 4, 30),
        batch_size=5,
        page_limit=1000,
    )

    assert table.num_rows == 3
    assert table.schema.equals(schemas.NEWS)
    assert len(calls) == 1  # 3 tickers fit in one batch_size=5 batch, 1 short page
    assert calls[0]["tickers"] == ["AAPL", "MSFT", "GOOGL"]


def test_fetch_universe_news_paginates_when_page_full(monkeypatch):
    """A full page triggers another fetch with bumped offset."""
    full_page = [_sample_article(i) for i in range(10)]
    short_page = [_sample_article(i + 100) for i in range(2)]

    def fake_fetch_news(**kwargs):
        return full_page if kwargs["offset"] == 0 else short_page

    monkeypatch.setattr(loader.tiingo, "fetch_news", fake_fetch_news)

    table = loader.fetch_universe_news(
        tickers=["AAPL"],
        start=dt.date(2025, 4, 1),
        end=dt.date(2025, 4, 30),
        batch_size=5,
        page_limit=10,
    )
    # 10 from page 1 + 2 from page 2 = 12 unique articles
    assert table.num_rows == 12


def test_fetch_universe_news_dedupes_within_run(monkeypatch):
    """Same article_id from two batches → only stored once."""
    duped = _sample_article(article_id=999)

    def fake_fetch_news(**kwargs):
        # Both batches return the same article id
        return [duped] if kwargs["offset"] == 0 else []

    monkeypatch.setattr(loader.tiingo, "fetch_news", fake_fetch_news)

    table = loader.fetch_universe_news(
        tickers=["AAPL", "MSFT", "GOOGL", "TSLA", "META", "NVDA"],
        start=dt.date(2025, 4, 1),
        end=dt.date(2025, 4, 30),
        batch_size=2,  # → 3 batches
        page_limit=1000,
    )
    assert table.num_rows == 1


def test_fetch_universe_news_continues_after_batch_failure(monkeypatch):
    """One batch raising should not abort subsequent batches."""
    state = {"call_count": 0}

    def fake_fetch_news(**kwargs):
        state["call_count"] += 1
        if state["call_count"] == 1:
            raise RuntimeError("simulated network failure")
        return [_sample_article(state["call_count"])]

    monkeypatch.setattr(loader.tiingo, "fetch_news", fake_fetch_news)

    table = loader.fetch_universe_news(
        tickers=["AAPL", "MSFT", "GOOGL", "TSLA"],
        start=dt.date(2025, 4, 1),
        end=dt.date(2025, 4, 30),
        batch_size=2,  # → 2 batches
        page_limit=1000,
    )
    # First batch raised; second succeeded with 1 article
    assert table.num_rows == 1


def test_fetch_universe_news_empty_returns_empty_table(monkeypatch):
    monkeypatch.setattr(loader.tiingo, "fetch_news", lambda **k: [])
    table = loader.fetch_universe_news(
        tickers=["AAPL"],
        start=dt.date(2025, 4, 1),
        end=dt.date(2025, 4, 30),
    )
    assert table.num_rows == 0
    assert table.schema.equals(schemas.NEWS)


# ─────────────────────────────────────────────────────────────────────────────
# write_news + partition layout
# ─────────────────────────────────────────────────────────────────────────────


def test_write_news_partitions_by_crawl_year(monkeypatch, tmp_path):
    """Articles crawled in different years should land in different partitions."""
    monkeypatch.setattr(
        loader.writer, "news_dir", lambda: tmp_path / "news",
    )

    articles = [
        _sample_article(1, crawlDate="2024-12-15T10:00:00Z"),
        _sample_article(2, crawlDate="2025-01-15T10:00:00Z"),
        _sample_article(3, crawlDate="2025-04-30T10:00:00Z"),
    ]

    rows = [loader._normalize_article(a, dt.date(2025, 5, 1)) for a in articles]
    table = pa.Table.from_pylist(rows, schema=schemas.NEWS)
    written = loader.write_news(table)

    assert len(written) == 2  # one for 2024, one for 2025
    paths = sorted(p.parent.name for p in written)
    assert paths == ["crawl_year=2024", "crawl_year=2025"]


def test_write_news_idempotent(monkeypatch, tmp_path):
    """Re-writing the same articles must not increase row count."""
    monkeypatch.setattr(
        loader.writer, "news_dir", lambda: tmp_path / "news",
    )

    articles = [_sample_article(i) for i in range(5)]
    rows = [loader._normalize_article(a, dt.date(2025, 5, 1)) for a in articles]
    table = pa.Table.from_pylist(rows, schema=schemas.NEWS)

    loader.write_news(table)
    loader.write_news(table)  # second write, same data

    # Find the resulting parquet
    parts = list((tmp_path / "news").glob("**/*.parquet"))
    assert len(parts) == 1
    final = pq.read_table(parts[0])
    assert final.num_rows == 5  # not 10 — dedup by article_id worked


def test_pagination_safety_cap_logs_warning(monkeypatch, caplog):
    """If a batch never returns a short page, we stop at MAX_PAGES_PER_BATCH and warn."""
    monkeypatch.setattr(loader, "MAX_PAGES_PER_BATCH", 3)

    state = {"id": 0}

    def never_short(**kwargs):
        # Always return a full page with fresh ids → loop never exits naturally
        state["id"] += 1
        return [_sample_article(state["id"] * 10000 + i) for i in range(10)]

    monkeypatch.setattr(loader.tiingo, "fetch_news", never_short)

    import logging
    with caplog.at_level(logging.WARNING, logger="pitds.news.loader"):
        table = loader.fetch_universe_news(
            tickers=["AAPL"],
            start=dt.date(2025, 4, 1),
            end=dt.date(2025, 4, 30),
            batch_size=1,
            page_limit=10,
        )

    # 3 pages × 10 articles each, all unique
    assert table.num_rows == 30
    assert any("safety cap" in r.message for r in caplog.records)


def test_warn_on_date_drift_flags_start_truncation(caplog):
    """Earliest returned article well after requested start → WARNING."""
    import logging
    with caplog.at_level(logging.WARNING, logger="pitds.news.loader"):
        loader._warn_on_date_drift(
            requested_start=dt.date(2020, 1, 1),
            requested_end=dt.date(2026, 5, 1),
            actual_min_crawl=dt.datetime(2025, 11, 1, tzinfo=dt.timezone.utc),
            actual_max_crawl=dt.datetime(2026, 4, 30, tzinfo=dt.timezone.utc),
        )
    msgs = [r.message for r in caplog.records]
    assert any("Earliest returned article" in m and "after" in m for m in msgs)


def test_warn_on_date_drift_flags_end_truncation(caplog):
    """Latest returned article well before requested end → WARNING."""
    import logging
    with caplog.at_level(logging.WARNING, logger="pitds.news.loader"):
        loader._warn_on_date_drift(
            requested_start=dt.date(2025, 1, 1),
            requested_end=dt.date(2026, 5, 1),
            actual_min_crawl=dt.datetime(2025, 1, 5, tzinfo=dt.timezone.utc),
            actual_max_crawl=dt.datetime(2025, 6, 1, tzinfo=dt.timezone.utc),
        )
    msgs = [r.message for r in caplog.records]
    assert any("Latest returned article" in m and "before" in m for m in msgs)


def test_warn_on_date_drift_silent_when_aligned(caplog):
    """Tight match between requested and actual → no warnings."""
    import logging
    with caplog.at_level(logging.WARNING, logger="pitds.news.loader"):
        loader._warn_on_date_drift(
            requested_start=dt.date(2025, 4, 1),
            requested_end=dt.date(2025, 4, 30),
            actual_min_crawl=dt.datetime(2025, 4, 1, tzinfo=dt.timezone.utc),
            actual_max_crawl=dt.datetime(2025, 4, 30, tzinfo=dt.timezone.utc),
        )
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert warnings == []


def test_warn_on_date_drift_warns_when_no_articles_returned(caplog):
    """Empty result with non-empty window → WARNING."""
    import logging
    with caplog.at_level(logging.WARNING, logger="pitds.news.loader"):
        loader._warn_on_date_drift(
            requested_start=dt.date(2025, 4, 1),
            requested_end=dt.date(2025, 4, 30),
            actual_min_crawl=None,
            actual_max_crawl=None,
        )
    msgs = [r.message for r in caplog.records]
    assert any("No articles returned" in m for m in msgs)


def test_class_share_tickers_normalized_for_tiingo(monkeypatch):
    """BRK.B / BF.B must be sent to Tiingo as brk-b / bf-b, not the dot form.

    Tiingo (like yfinance and Stooq) expects class-share tickers in dash form.
    We canonically store the dot form; the news client must normalize before
    sending. Without this, BRK.B and BF.B silently return zero articles.
    """
    captured: dict = {}

    class FakeResp:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return []

    def fake_get(url, *, headers, params, timeout):
        captured.update(params)
        return FakeResp()

    monkeypatch.setattr(tiingo_client.httpx, "get", fake_get)
    tiingo_client.fetch_news(tickers=["BRK.B", "BF.B", "AAPL"])

    assert captured["tickers"] == "brk-b,bf-b,aapl"


def test_daily_step_news_calls_backfill_with_2_day_window(monkeypatch):
    """The daily news step should request start = today - 2 days, end = today,
    over the full historical universe."""
    import pandas as pd
    from pitds.update import daily
    import pitds.news.loader as news_loader

    fake_snap = type("Snap", (), {
        "ticker_tenures": pd.DataFrame({"ticker": ["AAPL", "MSFT", "GOOGL"]}),
    })()
    monkeypatch.setattr(daily, "fetch_snapshot", lambda: fake_snap)

    captured: dict = {}

    def fake_backfill(tickers, *, start, end, batch_size):
        captured["tickers"] = tickers
        captured["start"] = start
        captured["end"] = end
        captured["batch_size"] = batch_size
        return 1234

    monkeypatch.setattr(news_loader, "backfill", fake_backfill)

    run = daily.DailyRun()
    daily._step_news_yesterday(run)

    assert (captured["end"] - captured["start"]).days == 2
    assert captured["end"] == dt.date.today()
    assert captured["batch_size"] == 5
    assert sorted(captured["tickers"]) == ["AAPL", "GOOGL", "MSFT"]
    assert len(run.steps) == 1
    assert run.steps[0].name == "news_yesterday"
    assert run.steps[0].succeeded is True
    assert run.steps[0].rows_written == 1234


def test_daily_step_news_handles_exception(monkeypatch):
    """If backfill raises, the step records failure but doesn't propagate."""
    from pitds.update import daily

    monkeypatch.setattr(
        daily, "fetch_snapshot",
        lambda: (_ for _ in ()).throw(RuntimeError("simulated snapshot failure")),
    )

    run = daily.DailyRun()
    daily._step_news_yesterday(run)
    assert len(run.steps) == 1
    assert run.steps[0].succeeded is False
    assert run.steps[0].error is not None


def test_schema_includes_required_pit_columns():
    """Sanity: NEWS schema must carry the columns the read API will rely on."""
    cols = set(schemas.NEWS.names)
    # PIT gate
    assert "crawl_date" in cols
    # Audit
    assert "ingestion_date" in cols
    # Source-claim, available for non-PIT analyses
    assert "published_date" in cols
    # Dedup key
    assert "article_id" in cols
    # Universe linkage
    assert "tickers" in cols
