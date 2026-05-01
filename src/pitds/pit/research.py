"""Helpers for PIT-correct ML / backtest research workflows.

These are thin wrappers over ``pit.api`` that bake in the correctness rules
that researchers most often get wrong:

1. Universe membership at the per-date as_of (not today's universe)
2. Feature lookback windows that strictly precede the prediction date
3. Vintage consistency between training and inference
4. Reporting-lag handling for fundamentals (a Q4 number isn't knowable
   until the 10-K is filed, ~45-90 days after period_end)
5. Pre-trade tradability filter (universe + minimum history + freshness)

Use these as building blocks; subclass / extend for project-specific rules.
"""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

import pandas as pd

from . import api as _api

log = logging.getLogger(__name__)

VintageMode = Literal["latest_known", "first_reported"]


# ─────────────────────────────────────────────────────────────────────────────
# Tradability / scoreability filter — use this BEFORE generating features
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ScoreabilityResult:
    cik: str
    ok: bool
    reason: str
    history_days: int = 0


def is_scoreable(
    cik: str,
    *,
    as_of: dt.date,
    min_history_days: int = 252,
    max_freshness_days: int = 5,
    require_universe_membership: bool = True,
) -> ScoreabilityResult:
    """Decide whether *cik* should be scored on *as_of*.

    Implements the universal pre-trade filter described in DESIGN.md §15.

    Returns a :class:`ScoreabilityResult` rather than raising, so callers
    can log skipped names without try/except noise.
    """
    cik = str(cik).zfill(10)

    if require_universe_membership:
        universe = _api.constituents(as_of=as_of)
        if cik not in set(universe["cik"].dropna()):
            return ScoreabilityResult(
                cik=cik, ok=False,
                reason="not in S&P 500 universe on as_of",
            )

    hist = _api.price_history(cik=cik, end=as_of, adjust="none")
    if len(hist) < min_history_days:
        return ScoreabilityResult(
            cik=cik, ok=False,
            reason=f"only {len(hist)} days of price history (need {min_history_days})",
            history_days=len(hist),
        )

    last_date = hist["date"].max()
    if hasattr(last_date, "date"):
        last_date = last_date.date()
    stale_days = (as_of - last_date).days
    if stale_days > max_freshness_days:
        return ScoreabilityResult(
            cik=cik, ok=False,
            reason=f"latest price is {stale_days} days stale (>{max_freshness_days})",
            history_days=len(hist),
        )

    return ScoreabilityResult(cik=cik, ok=True, reason="ok", history_days=len(hist))


def filter_universe(
    *,
    as_of: dt.date,
    min_history_days: int = 252,
    max_freshness_days: int = 5,
) -> list[str]:
    """Return the list of CIKs scoreable on *as_of*. Convenience wrapper."""
    universe = _api.constituents(as_of=as_of)
    out: list[str] = []
    for cik in universe["cik"].dropna():
        r = is_scoreable(
            cik, as_of=as_of,
            min_history_days=min_history_days,
            max_freshness_days=max_freshness_days,
            require_universe_membership=False,  # already filtered
        )
        if r.ok:
            out.append(r.cik)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Feature retrieval — strictly PIT-correct for a single (cik, as_of)
# ─────────────────────────────────────────────────────────────────────────────


def latest_reported_fact(
    *,
    cik: str,
    field_name: str,
    as_of: dt.date,
    max_lookback_days: int = 540,
    vintage: VintageMode = "latest_known",
    period_kind: Literal["quarterly", "ytd", "annual", "any"] = "quarterly",
) -> pd.DataFrame:
    """Return the most recent fact for *field_name* knowable on *as_of*.

    Single SQL query against the fundamentals partition tree, filtered by:

    * ``filed <= as_of`` (PIT cutoff)
    * any of *field_name*'s candidate XBRL concepts (concept aliasing)
    * ``period_end <= as_of`` and within max_lookback_days
    * period span matching *period_kind*

    Use ``period_kind`` to disambiguate XBRL's overlapping period reports
    for the same period_end:

    * ``quarterly`` (default) — 80-100 day period (one fiscal quarter).
    * ``ytd`` — < 80d or between 100-350d (cumulative since FY start).
    * ``annual`` — 350-380 day period (full fiscal year).
    * ``any`` — no period-length filter; takes whatever XBRL has.

    Instantaneous facts (balance sheet: total_assets, shares_outstanding,
    cash, total_equity) have period_start = NULL and are returned regardless
    of period_kind.
    """
    import duckdb

    from ..fundamentals import concepts as concepts_module
    from ..store import writer as _writer

    cik = str(cik).zfill(10)
    facts_dir = _writer.fundamentals_facts_dir()
    glob = str(facts_dir / "**" / "*.parquet")
    candidate_concepts = concepts_module.concepts_for(field_name)
    concept_in = ",".join(f"'{c}'" for c in candidate_concepts)
    earliest_period = (as_of - dt.timedelta(days=max_lookback_days)).isoformat()

    if period_kind == "quarterly":
        span_clause = (
            "AND period_start IS NOT NULL "
            "AND date_diff('day', period_start, period_end) BETWEEN 80 AND 100"
        )
    elif period_kind == "annual":
        span_clause = (
            "AND period_start IS NOT NULL "
            "AND date_diff('day', period_start, period_end) BETWEEN 350 AND 380"
        )
    elif period_kind == "ytd":
        span_clause = (
            "AND period_start IS NOT NULL "
            "AND (date_diff('day', period_start, period_end) < 80 "
            "     OR date_diff('day', period_start, period_end) BETWEEN 100 AND 350)"
        )
    else:  # 'any'
        span_clause = ""

    # For balance-sheet items the natural span is "instantaneous"
    # (period_start IS NULL). Always include those as a safety net so
    # total_assets / shares_outstanding work without explicit period_kind.
    instant_or_span = (
        f"((period_start IS NULL) OR (period_start IS NOT NULL {span_clause}))"
        if span_clause else "TRUE"
    )

    order = "DESC" if vintage == "latest_known" else "ASC"

    sql = f"""
    SELECT * FROM read_parquet({glob!r}, hive_partitioning=true)
    WHERE cik = '{cik}'
      AND concept IN ({concept_in})
      AND filed <= DATE '{as_of.isoformat()}'
      AND period_end <= DATE '{as_of.isoformat()}'
      AND period_end >= DATE '{earliest_period}'
      AND {instant_or_span}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cik, concept, period_start, period_end
        ORDER BY filed {order}
    ) = 1
    ORDER BY period_end DESC, concept
    LIMIT 1
    """
    con = duckdb.connect()
    df = con.sql(sql).df()
    con.close()
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Train/test split — the canonical pattern
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class WalkForwardSplit:
    """A single train/test fold."""

    train_start: dt.date
    train_end: dt.date
    test_start: dt.date
    test_end: dt.date


def walk_forward_splits(
    *,
    start: dt.date,
    end: dt.date,
    train_years: int = 5,
    test_months: int = 6,
    step_months: int = 6,
) -> list[WalkForwardSplit]:
    """Generate non-overlapping walk-forward train/test folds.

    Default: 5y train, 6mo test, 6mo step. Common in factor research;
    avoids the look-ahead leak of a single train/test split where the
    test period includes data restated *during* training.
    """
    folds: list[WalkForwardSplit] = []
    train_start = start
    while True:
        # Approximate year/month arithmetic — fine for the granularity we need.
        train_end = train_start.replace(year=train_start.year + train_years) - dt.timedelta(days=1)
        test_start = train_end + dt.timedelta(days=1)
        test_end_year = test_start.year + (test_start.month - 1 + test_months) // 12
        test_end_month = ((test_start.month - 1 + test_months) % 12) + 1
        try:
            test_end = test_start.replace(
                year=test_end_year, month=test_end_month
            ) - dt.timedelta(days=1)
        except ValueError:
            test_end = end
        if test_end > end:
            break
        folds.append(WalkForwardSplit(
            train_start=train_start, train_end=train_end,
            test_start=test_start, test_end=test_end,
        ))
        # Advance by step_months
        new_year = train_start.year + (train_start.month - 1 + step_months) // 12
        new_month = ((train_start.month - 1 + step_months) % 12) + 1
        train_start = train_start.replace(year=new_year, month=new_month)
    return folds


# ─────────────────────────────────────────────────────────────────────────────
# Sanity checks for a research run
# ─────────────────────────────────────────────────────────────────────────────


def assert_pit_safe_query(
    *,
    feature_as_of: dt.date,
    label_period_end: dt.date | None = None,
    constituent_as_of: dt.date | None = None,
):
    """Catch the most common PIT bugs at call time.

    Pass it your feature_as_of, label_period_end (if any), and the date you
    queried constituents for. It raises if any are inconsistent.
    """
    if label_period_end is not None and label_period_end <= feature_as_of:
        raise ValueError(
            f"Label period_end ({label_period_end}) must be > feature_as_of "
            f"({feature_as_of}); otherwise you're predicting the past."
        )
    if constituent_as_of is not None and constituent_as_of != feature_as_of:
        raise ValueError(
            f"Constituent as_of ({constituent_as_of}) should equal "
            f"feature as_of ({feature_as_of}) — otherwise universe leaks."
        )
