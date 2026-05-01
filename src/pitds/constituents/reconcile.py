"""Three-source constituent reconciliation: fja05680 vs Wikipedia vs cross-source.

This module surfaces *anomalies* — places where our sources disagree — for
manual review. It does NOT auto-apply differences; the operator must decide
based on the official S&P press releases (free at spglobal.com).

Phase A scope: just the diff. Phase C wires this into a daily updater that
emails or logs anomalies.
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass

import pandas as pd

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConstituentDiff:
    """Tickers in one source but not the other, on the comparison date."""

    as_of: dt.date
    only_in_fja: set[str]
    only_in_wiki: set[str]


def diff_current_state(
    fja_current: pd.DataFrame,
    wiki_current: pd.DataFrame,
) -> ConstituentDiff:
    """Compute the symmetric difference between fja and Wikipedia current states.

    Both inputs must have a ``symbol`` column (Wikipedia) or ``ticker``
    column (fja). We unify before comparing.
    """
    fja_col = "symbol" if "symbol" in fja_current.columns else "ticker"
    wiki_col = "symbol" if "symbol" in wiki_current.columns else "ticker"

    fja_set = set(fja_current[fja_col].astype(str).str.strip())
    wiki_set = set(wiki_current[wiki_col].astype(str).str.strip())

    return ConstituentDiff(
        as_of=dt.date.today(),
        only_in_fja=fja_set - wiki_set,
        only_in_wiki=wiki_set - fja_set,
    )
