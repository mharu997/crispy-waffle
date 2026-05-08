"""Nightly orchestrator — keeps the dataset PIT-correct as time advances.

Idempotent: re-running on the same day is a no-op except for actually-new
data. Steps run sequentially; failure of any single step is logged but
does not halt the rest, so a transient yfinance outage doesn't block the
EDGAR pull.

Cron-friendly schedule (post-market-close ET, ~21:00 UTC):
    21 0 * * 2-6  cd /path/to/PITDS && .venv/bin/pitds update-daily

The update enforces the three PIT-correctness rules from the plan:

1. Append-only writes — no historical row is overwritten.
2. Vintage-stamped Parquet metadata — every file's ``ingestion_date`` is
   today, so backtests can be replicated exactly by reading the on-disk
   vintage.
3. Anomaly surfacing, not silent fixes — a constituent diff that doesn't
   match S&P press releases is *logged*, not auto-applied.
"""

from __future__ import annotations

import datetime as dt
import logging
import traceback
from dataclasses import dataclass, field

from ..constituents.fja05680 import fetch_snapshot
from ..constituents.reconcile import diff_current_state
from ..identifiers.cik_map import resolve_ciks
from . import notify
from .verify import format_report, run_all

log = logging.getLogger(__name__)


@dataclass
class StepResult:
    name: str
    succeeded: bool
    message: str
    rows_written: int = 0
    error: str | None = None


@dataclass
class DailyRun:
    started: dt.datetime = field(default_factory=dt.datetime.now)
    steps: list[StepResult] = field(default_factory=list)

    def add(self, name: str, ok: bool, msg: str, rows: int = 0, err: str | None = None):
        self.steps.append(
            StepResult(name=name, succeeded=ok, message=msg, rows_written=rows, error=err)
        )

    def report(self) -> str:
        lines = [
            f"PITDS daily update — started {self.started.isoformat(timespec='seconds')}",
            "=" * 70,
        ]
        for s in self.steps:
            tag = "OK" if s.succeeded else "FAIL"
            lines.append(f"  [{tag:4}] {s.name:30} {s.message} (rows={s.rows_written})")
            if s.error:
                lines.append(f"          ERROR: {s.error}")
        n_ok = sum(1 for s in self.steps if s.succeeded)
        lines.append("-" * 70)
        lines.append(f"  {n_ok}/{len(self.steps)} steps succeeded")
        return "\n".join(lines)


def _step_constituents_check(run: DailyRun) -> None:
    """Check fja05680 + Wikipedia for new constituent changes."""
    name = "constituents_diff"
    try:
        from ..constituents.wikipedia import fetch_current_constituents

        snap = fetch_snapshot(refresh=True)
        wiki = fetch_current_constituents()
        diff = diff_current_state(snap.current_snapshot, wiki)
        if diff.only_in_fja or diff.only_in_wiki:
            msg = (
                f"DIFF! only_in_fja={sorted(diff.only_in_fja)[:5]} "
                f"only_in_wiki={sorted(diff.only_in_wiki)[:5]}"
            )
            notify.notify(
                "constituent_diff",
                f"fja-only={sorted(diff.only_in_fja)}, wiki-only={sorted(diff.only_in_wiki)}",
                severity="warning",
            )
            run.add(name, False, msg)
        else:
            run.add(name, True, "no diff")
    except Exception as e:
        run.add(name, False, "exception", err=str(e))


def _step_constituent_materialize(run: DailyRun) -> None:
    """Re-materialize constituents_daily up to today (idempotent)."""
    name = "constituents_materialize"
    try:
        from ..constituents.changes import write_constituents_outputs

        snap = fetch_snapshot()
        n_daily, n_events = write_constituents_outputs(
            snap.ticker_tenures, snap.current_snapshot
        )
        run.add(name, True, f"{n_daily:,} daily rows, {n_events} change events", rows=n_daily)
    except Exception as e:
        run.add(name, False, "exception", err=traceback.format_exc().splitlines()[-1])


def _step_prices_yesterday(run: DailyRun) -> None:
    """Fetch yesterday's prices for the current constituent universe."""
    name = "prices_yesterday"
    try:
        from ..prices.loader import backfill

        snap = fetch_snapshot()
        tickers = sorted(snap.current_snapshot["symbol"].astype(str).str.upper())
        seed = dict(
            zip(
                snap.current_snapshot["symbol"].astype(str).str.upper(),
                snap.current_snapshot["cik"].astype(str),
                strict=False,
            )
        )
        seed = {k: v for k, v in seed.items() if v and v != "<NA>" and v != "nan"}
        cik_map = resolve_ciks(tickers, seed=seed)
        end = dt.date.today()
        start = end - dt.timedelta(days=4)  # safety margin for weekends
        n = backfill(
            tickers,
            start=start,
            end=end,
            cik_resolver=lambda t: cik_map.get(t.upper()),
        )
        run.add(name, True, f"backfilled [{start}, {end}]", rows=n)
    except Exception as e:
        run.add(name, False, "exception", err=traceback.format_exc().splitlines()[-1])


def _step_corporate_actions_yesterday(run: DailyRun) -> None:
    """Fetch any new corporate actions for the current universe."""
    name = "corp_actions_yesterday"
    try:
        from ..corporate_actions.splits_dividends import build_actions_table
        from ..store import writer as w

        snap = fetch_snapshot()
        tickers = sorted(snap.current_snapshot["symbol"].astype(str).str.upper())
        seed = dict(
            zip(
                snap.current_snapshot["symbol"].astype(str).str.upper(),
                snap.current_snapshot["cik"].astype(str),
                strict=False,
            )
        )
        seed = {k: v for k, v in seed.items() if v and v != "<NA>" and v != "nan"}
        cik_map = resolve_ciks(tickers, seed=seed)
        # Quick mode: only fetch tickers that had a likely action in last 7 days.
        # Since yfinance returns full history per call, we just refresh all.
        table = build_actions_table(
            tickers, cik_resolver=lambda t: cik_map.get(t.upper())
        )
        if table.num_rows == 0:
            run.add(name, True, "no new actions")
            return
        w.upsert_single_file(
            table, w.corporate_actions_path(),
            primary_key=["ticker", "ex_date", "action_type"],
            sort_by=["ticker", "ex_date"],
        )
        run.add(name, True, "refreshed corp actions", rows=table.num_rows)
    except Exception as e:
        run.add(name, False, "exception", err=traceback.format_exc().splitlines()[-1])


def _step_fundamentals_incremental(run: DailyRun) -> None:
    """Check EDGAR submissions for any CIK with a new filing since last run."""
    name = "fundamentals_incremental"
    try:
        # Phase C placeholder: a full implementation diffs each CIK's
        # submissions.json `recent.filings.filing_date[0]` against our
        # last-seen, then re-pulls companyfacts only for advanced CIKs.
        # For now, defer to weekly batch.
        run.add(name, True, "deferred to weekly batch")
    except Exception as e:
        run.add(name, False, "exception", err=str(e))


def _step_news_yesterday(run: DailyRun) -> None:
    """Pull the last 4 days of Tiingo news for the historical S&P universe.

    The window must cover the worst-case gap between consecutive cron runs:

    * **Weekday → next weekday**: ~24h gap, plus crawler boundary smear of
      a few hours (an article published 23:50 UTC may have crawl_date
      00:05 UTC the next day) → 2 days suffices.
    * **Friday → Monday**: ~72h gap (3 days), so a 2-day window would
      silently miss articles published Friday between cron-run-time
      (17:30 ET) and end-of-day. A 4-day window covers Thursday-onwards
      from Monday's run, absorbing the weekend reliably.
    * **Holiday Monday → Tuesday**: 4-day window still covers Friday's
      post-cron tail. Two consecutive holidays would need extending —
      acceptable trade-off given how rare that is.

    Universe is "all" (historical) rather than "current" so that delisted
    tickers with rare news (e.g., bankruptcy proceedings for legacy names)
    are still picked up. Cost stays bounded by the startDate filter, which
    Tiingo honors for recent dates — the 4-day window pulls ~50-80
    paginated requests per run, well under the daily budget. article_id
    dedup at write time absorbs the redundancy from overlapping windows.
    """
    name = "news_yesterday"
    try:
        from ..news.loader import backfill

        snap = fetch_snapshot()
        tickers = sorted(set(snap.ticker_tenures["ticker"].astype(str).str.upper()))
        end = dt.date.today()
        start = end - dt.timedelta(days=4)
        n = backfill(tickers, start=start, end=end, batch_size=5)
        run.add(name, True, f"backfilled [{start}, {end}]", rows=n)
    except Exception as e:
        run.add(name, False, "exception", err=traceback.format_exc().splitlines()[-1])


def _step_verify(run: DailyRun) -> None:
    """Run the integrity check suite."""
    name = "verify"
    try:
        results = run_all()
        n_pass = sum(1 for r in results if r.passed)
        msg = f"{n_pass}/{len(results)} checks passed"
        critical_fails = [r for r in results if not r.passed and r.severity == "critical"]
        if critical_fails:
            msg += f" — CRITICAL: {[r.name for r in critical_fails]}"
            run.add(name, False, msg)
        else:
            run.add(name, True, msg)
    except Exception as e:
        run.add(name, False, "exception", err=str(e))


STEPS = [
    _step_constituents_check,
    _step_constituent_materialize,
    _step_prices_yesterday,
    _step_corporate_actions_yesterday,
    _step_fundamentals_incremental,
    _step_news_yesterday,
    _step_verify,
]


def run_daily() -> DailyRun:
    """Execute all daily-update steps; return a DailyRun summary."""
    run = DailyRun()
    for step in STEPS:
        try:
            step(run)
        except Exception as e:
            run.add(step.__name__, False, "uncaught exception", err=str(e))
    log.info(run.report())
    return run
