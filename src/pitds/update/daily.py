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
3. Anomaly surfacing, not silent fixes — the fja-vs-Wikipedia diff is
   reported every run. Membership changes newer than the fja snapshot are
   applied from Wikipedia as explicitly-tagged ``wikipedia_live_edge``
   corrections (dated by revision bisection) so current membership is
   correct; they are superseded as soon as fja catches up.
"""

from __future__ import annotations

import datetime as dt
import logging
import traceback
from dataclasses import dataclass, field

from ..constituents.fja05680 import fetch_snapshot
from ..constituents.reconcile import diff_current_state
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


def _step_identity_rebuild(run: DailyRun) -> None:
    """Live-edge membership → security master → prices/actions → constituents → sectors.

    Replaces the old ticker-keyed price/corporate-action steps, which fetched
    by *current* ticker and so silently attached the wrong company's history
    to renamed or reused tickers. See update/identity.py.
    """
    name = "identity_rebuild"
    try:
        from .identity import rebuild_identity

        r = rebuild_identity(live_edge=True, prices=True, fundamentals_for_new=True)
        msg = (
            f"live_edge={r.live_edge_rows} sm={r.security_master_rows} "
            f"status={r.price_status} prices={r.price_rows:,} actions={r.action_rows:,} "
            f"new_fund_ciks={r.new_fundamentals_ciks}"
        )
        run.add(name, True, msg, rows=r.price_rows)
    except Exception:
        run.add(name, False, "exception", err=traceback.format_exc().splitlines()[-1])


def _step_fundamentals_incremental(run: DailyRun) -> None:
    """Re-pull EDGAR facts for security-master CIKs that filed since the last run.

    Uses EDGAR's daily master index (one request per business day) to find
    filers; see fundamentals/incremental.py.
    """
    name = "fundamentals_incremental"
    try:
        from ..fundamentals.incremental import refresh_changed
        from ..identifiers.security_master import load_security_master

        n, since, until = refresh_changed(load_security_master())
        run.add(name, True, f"{n} CIKs refreshed for filings in ({since}, {until}]", rows=n)
    except Exception:
        run.add(name, False, "exception", err=traceback.format_exc().splitlines()[-1])


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
    _step_identity_rebuild,
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
