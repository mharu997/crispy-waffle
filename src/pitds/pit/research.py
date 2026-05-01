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
    max_lookback_days: int = 365,
    vintage: VintageMode = "latest_known",
) -> pd.DataFrame:
    """Return the most recent (period_end <= as_of) fact for *field_name*.

    Handles the reporting-lag problem: you ask for "the latest revenue
    knowable on 2024-06-28," and we walk back through period_end candidates
    to find one whose filing was made on or before 2024-06-28.

    Set ``vintage='first_reported'`` for "as originally reported" semantics
    (use this when you want zero look-ahead from restatements).
    """
    cik = str(cik).zfill(10)
    # Walk back at most max_lookback_days; for canonical periods we expect
    # quarter ends so we sample a few likely period_ends.
    candidates: list[dt.date] = []
    cursor = as_of
    while (as_of - cursor).days <= max_lookback_days:
        candidates.append(cursor)
        cursor -= dt.timedelta(days=30)

    for period_end in candidates:
        df = _api.fact(
            cik=cik, field_name=field_name,
            period_end=period_end, as_of=as_of, vintage=vintage,
        )
        if not df.empty:
            return df
    return pd.DataFrame()


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
