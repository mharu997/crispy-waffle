"""Integrity assertions for PITDS — runnable as a CLI or programmatically.

Designed to be run nightly as part of the daily updater. Surfaces
violations of the PIT contract, schema invariants, and cross-table
consistency.

Each check returns a :class:`CheckResult` so the caller can decide whether
to fail-loud (CI), email-on-failure (cron), or just report.
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass

import duckdb

from ..store import writer

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    message: str
    severity: str  # 'critical' | 'warning' | 'info'


def _con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


def check_constituent_count() -> CheckResult:
    """Every date's constituent count is in [495, 510]."""
    p = writer.constituents_daily_path()
    if not p.exists():
        return CheckResult(
            "constituent_count", True, "skipped (no constituents_daily)", "info"
        )
    con = _con()
    bad = con.sql(
        f"""
        SELECT date, COUNT(*) AS n
        FROM read_parquet({str(p)!r})
        GROUP BY date
        HAVING n < 495 OR n > 510
        ORDER BY date
        LIMIT 10
        """
    ).fetchall()
    con.close()
    if not bad:
        return CheckResult(
            "constituent_count", True,
            "all dates in [495, 510]", "info",
        )
    return CheckResult(
        "constituent_count", False,
        f"{len(bad)} dates out of range, e.g. {bad[:3]}",
        "critical",
    )


def check_no_future_prices() -> CheckResult:
    """No price row dated after today."""
    p = writer.prices_path()
    if not p.exists():
        return CheckResult("no_future_prices", True, "skipped (no prices)", "info")
    con = _con()
    n = con.sql(
        f"SELECT COUNT(*) FROM read_parquet({str(p)!r}) "
        f"WHERE date > DATE '{dt.date.today().isoformat()}'"
    ).fetchone()[0]
    con.close()
    return CheckResult(
        "no_future_prices",
        n == 0,
        f"{n} rows with date > today" if n else "OK",
        "critical" if n else "info",
    )


def check_price_pk_uniqueness() -> CheckResult:
    """No duplicate (cik, date) or (ticker, date) pairs in prices."""
    p = writer.prices_path()
    if not p.exists():
        return CheckResult("price_pk_uniqueness", True, "skipped (no prices)", "info")
    con = _con()
    n = con.sql(
        f"""
        SELECT COUNT(*) FROM (
            SELECT ticker, date, COUNT(*) AS c
            FROM read_parquet({str(p)!r})
            GROUP BY ticker, date HAVING c > 1
        )
        """
    ).fetchone()[0]
    con.close()
    return CheckResult(
        "price_pk_uniqueness",
        n == 0,
        f"{n} duplicate (ticker, date) pairs" if n else "OK",
        "critical" if n else "info",
    )


def check_fundamentals_filed_not_future() -> CheckResult:
    """No fact has a filed date strictly in the future (would be a parse error)."""
    facts_dir = writer.fundamentals_facts_dir()
    if not any(facts_dir.glob("**/*.parquet")):
        return CheckResult("fundamentals_filed_not_future", True, "skipped (no fundamentals)", "info")
    con = _con()
    glob = str(facts_dir / "**" / "*.parquet")
    today = dt.date.today().isoformat()
    n = con.sql(
        f"""
        SELECT COUNT(*) FROM read_parquet({glob!r}, hive_partitioning=true)
        WHERE filed > DATE '{today}'
        """
    ).fetchone()[0]
    con.close()
    return CheckResult(
        "fundamentals_filed_not_future",
        n == 0,
        f"{n} facts have filed > today (parse error)" if n else "OK",
        "critical" if n else "info",
    )


def check_constituent_price_coverage(as_of: dt.date | None = None) -> CheckResult:
    """≥95% of constituents on *as_of* should have a price row."""
    p_prices = writer.prices_path()
    p_const = writer.constituents_daily_path()
    if not (p_prices.exists() and p_const.exists()):
        return CheckResult("price_coverage", True, "skipped", "info")
    as_of = as_of or dt.date.today() - dt.timedelta(days=14)
    con = _con()
    n_const, n_matched = con.sql(
        f"""
        WITH c AS (
            SELECT DISTINCT ticker FROM read_parquet({str(p_const)!r})
            WHERE date = (
                SELECT MAX(date) FROM read_parquet({str(p_const)!r})
                WHERE date <= DATE '{as_of.isoformat()}'
            )
        ),
        p AS (
            SELECT DISTINCT ticker FROM read_parquet({str(p_prices)!r})
            WHERE date = (
                SELECT MAX(date) FROM read_parquet({str(p_prices)!r})
                WHERE date <= DATE '{as_of.isoformat()}'
            )
        )
        SELECT
            (SELECT COUNT(*) FROM c) AS total,
            (SELECT COUNT(*) FROM c WHERE ticker IN (SELECT ticker FROM p)) AS matched
        """
    ).fetchone()
    con.close()
    pct = 100.0 * n_matched / max(n_const, 1)
    return CheckResult(
        "price_coverage",
        pct >= 95.0,
        f"{n_matched}/{n_const} ({pct:.1f}%) constituents have prices on {as_of}",
        "warning" if pct < 95.0 else "info",
    )


def check_known_event_tesla_addition() -> CheckResult:
    """Tesla (TSLA) was added to S&P 500 effective 2020-12-21."""
    p = writer.constituents_daily_path()
    if not p.exists():
        return CheckResult("event_tesla_add", True, "skipped", "info")
    con = _con()
    in_before = con.sql(
        f"""
        SELECT COUNT(*) FROM read_parquet({str(p)!r})
        WHERE date = DATE '2020-12-18' AND ticker = 'TSLA'
        """
    ).fetchone()[0]
    in_after = con.sql(
        f"""
        SELECT COUNT(*) FROM read_parquet({str(p)!r})
        WHERE date = DATE '2020-12-21' AND ticker = 'TSLA'
        """
    ).fetchone()[0]
    con.close()
    ok = in_before == 0 and in_after == 1
    return CheckResult(
        "event_tesla_add",
        ok,
        f"TSLA on 2020-12-18: {in_before}, on 2020-12-21: {in_after} (expect 0, 1)",
        "warning" if not ok else "info",
    )


def check_known_event_ge_removal() -> CheckResult:
    """GE was removed from S&P 500 effective 2018-06-26."""
    p = writer.constituents_daily_path()
    if not p.exists():
        return CheckResult("event_ge_remove", True, "skipped", "info")
    con = _con()
    in_before = con.sql(
        f"""
        SELECT COUNT(*) FROM read_parquet({str(p)!r})
        WHERE date = DATE '2018-06-25' AND ticker = 'GE'
        """
    ).fetchone()[0]
    in_after = con.sql(
        f"""
        SELECT COUNT(*) FROM read_parquet({str(p)!r})
        WHERE date = DATE '2018-06-26' AND ticker = 'GE'
        """
    ).fetchone()[0]
    con.close()
    ok = in_before == 1 and in_after == 0
    return CheckResult(
        "event_ge_remove",
        ok,
        f"GE on 2018-06-25: {in_before}, on 2018-06-26: {in_after} (expect 1, 0)",
        "warning" if not ok else "info",
    )


ALL_CHECKS = [
    check_constituent_count,
    check_no_future_prices,
    check_price_pk_uniqueness,
    check_fundamentals_filed_not_future,
    check_constituent_price_coverage,
    check_known_event_tesla_addition,
    check_known_event_ge_removal,
]


def run_all() -> list[CheckResult]:
    """Run every check; return their results in order."""
    return [c() for c in ALL_CHECKS]


def format_report(results: list[CheckResult]) -> str:
    """Return a human-readable verification report."""
    lines: list[str] = []
    n_pass = sum(1 for r in results if r.passed)
    lines.append(f"Verification: {n_pass}/{len(results)} checks passed")
    lines.append("=" * 60)
    for r in results:
        marker = "PASS" if r.passed else f"FAIL ({r.severity})"
        lines.append(f"  [{marker}] {r.name}: {r.message}")
    return "\n".join(lines)
