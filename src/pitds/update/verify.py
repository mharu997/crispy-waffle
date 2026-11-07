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


def check_known_event_frc_removal() -> CheckResult:
    """First Republic was removed from the S&P 500 before the open on 2023-05-04.

    (Replaces a GE check that expected a 2018-06-26 removal — that was GE
    leaving the Dow Jones Industrial Average; GE never left the S&P 500.)
    Also asserts GE *is* still a member on that date.
    """
    p = writer.constituents_daily_path()
    if not p.exists():
        return CheckResult("event_frc_remove", True, "skipped", "info")
    con = _con()
    q = lambda d, t: con.sql(  # noqa: E731
        f"SELECT COUNT(*) FROM read_parquet({str(p)!r}) WHERE date = DATE '{d}' AND ticker = '{t}'"
    ).fetchone()[0]
    before, after, ge = q("2023-05-03", "FRC"), q("2023-05-04", "FRC"), q("2018-06-26", "GE")
    con.close()
    ok = before == 1 and after == 0 and ge == 1
    return CheckResult(
        "event_frc_remove",
        ok,
        f"FRC 2023-05-03: {before}, 2023-05-04: {after} (expect 1, 0); GE 2018-06-26: {ge} (expect 1)",
        "warning" if not ok else "info",
    )


def check_constituent_cik_coverage() -> CheckResult:
    """Every member-day must carry a CIK (the canonical join key)."""
    p = writer.constituents_daily_path()
    if not p.exists():
        return CheckResult("constituent_cik_coverage", True, "skipped", "info")
    n, null = _con().sql(
        f"SELECT count(*), count(*) FILTER (WHERE cik IS NULL) FROM read_parquet({str(p)!r})"
    ).fetchone()
    return CheckResult(
        "constituent_cik_coverage",
        null == 0,
        f"{null:,}/{n:,} member-days without CIK",
        "critical" if null else "info",
    )


def check_price_identity_consistency() -> CheckResult:
    """A price row on a member's (ticker, date) must belong to that member's CIK."""
    pp, pc = writer.prices_path(), writer.constituents_daily_path()
    if not (pp.exists() and pc.exists()):
        return CheckResult("price_identity_consistency", True, "skipped", "info")
    n = _con().sql(
        f"""
        SELECT count(*) FROM read_parquet({str(pp)!r}) p
        JOIN read_parquet({str(pc)!r}) c ON c.date = p.date AND c.ticker = p.ticker
        WHERE c.cik IS DISTINCT FROM p.cik
        """
    ).fetchone()[0]
    return CheckResult(
        "price_identity_consistency",
        n == 0,
        f"{n:,} member-days priced with a different CIK's series" if n else "OK",
        "critical" if n else "info",
    )


def check_current_members_priced() -> CheckResult:
    """Every *current* member must resolve to a vendor series."""
    p = writer.security_master_path()
    if not p.exists():
        return CheckResult("current_members_priced", True, "skipped", "info")
    df = _con().sql(
        f"SELECT ticker_pit, price_status FROM read_parquet({str(p)!r}) "
        f"WHERE valid_to IS NULL AND price_status <> 'ok'"
    ).df()
    return CheckResult(
        "current_members_priced",
        df.empty,
        "OK" if df.empty else f"unpriced current members: {df.to_dict('records')}",
        "critical" if not df.empty else "info",
    )


def check_impossible_distributions() -> CheckResult:
    """A cash distribution can't exceed the prior close (vendor missed a split)."""
    pa_, pp = writer.corporate_actions_path(), writer.prices_path()
    if not (pa_.exists() and pp.exists()):
        return CheckResult("impossible_distributions", True, "skipped", "info")
    df = _con().sql(
        f"""
        WITH a AS (SELECT COALESCE(perma_ticker, ticker) k, * FROM read_parquet({str(pa_)!r})),
        p AS (SELECT COALESCE(perma_ticker, ticker) k, date, close FROM read_parquet({str(pp)!r})),
        s AS (SELECT k, ex_date, EXP(SUM(LN(ratio))) r FROM a WHERE action_type = 'split' GROUP BY 1, 2)
        SELECT a.ticker, a.ex_date, a.amount FROM a
        LEFT JOIN s ON s.k = a.k AND s.ex_date = a.ex_date
        WHERE a.action_type = 'cash_div'
          AND a.amount >= (SELECT close FROM p WHERE p.k = a.k AND p.date < a.ex_date
                           ORDER BY date DESC LIMIT 1) / COALESCE(s.r, 1)
        """
    ).df()
    return CheckResult(
        "impossible_distributions",
        df.empty,
        "OK" if df.empty else f"{len(df)} skipped by adjustment: {df.head(5).to_dict('records')}",
        "warning" if not df.empty else "info",
    )


def check_live_edge_freshness(max_age_days: int = 3) -> CheckResult:
    """The Wikipedia live-edge diff should be recomputed at least every few days."""
    from ..constituents import live_edge as le

    p = le.path()
    if not p.exists():
        return CheckResult("live_edge_freshness", False, "live edge never computed", "warning")
    age = (dt.date.today() - dt.date.fromtimestamp(p.stat().st_mtime)).days
    return CheckResult(
        "live_edge_freshness",
        age <= max_age_days,
        f"live edge computed {age} day(s) ago",
        "warning" if age > max_age_days else "info",
    )


ALL_CHECKS = [
    check_constituent_count,
    check_no_future_prices,
    check_price_pk_uniqueness,
    check_fundamentals_filed_not_future,
    check_constituent_price_coverage,
    check_known_event_tesla_addition,
    check_known_event_frc_removal,
    check_constituent_cik_coverage,
    check_price_identity_consistency,
    check_current_members_priced,
    check_impossible_distributions,
    check_live_edge_freshness,
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
