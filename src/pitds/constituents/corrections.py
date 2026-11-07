"""Curated corrections to the fja05680 ticker tenures.

See ``data/membership_corrections.csv`` for the rows and their evidence.
Applied once, at snapshot load time, so every consumer (constituents
materializer, security master, price/news backfills) sees the same
corrected membership.
"""

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

CORRECTIONS_PATH = Path(__file__).with_name("data") / "membership_corrections.csv"


def load_corrections() -> pd.DataFrame:
    if not CORRECTIONS_PATH.exists():
        return pd.DataFrame(columns=["action", "ticker", "start_date", "end_date", "note"])
    df = pd.read_csv(CORRECTIONS_PATH, comment="#", dtype=str).fillna("")
    df["ticker"] = df["ticker"].str.upper().str.strip()
    for c in ("start_date", "end_date"):
        df[c] = df[c].map(lambda v: dt.date.fromisoformat(v) if v else None)
    return df


def _cut(start, end, cut_from, cut_to):
    """Remove [cut_from, cut_to) from [start, end); return 0-2 intervals."""
    end_ = end if end is not None else dt.date.max
    out = []
    if start < cut_from:
        out.append((start, min(end_, cut_from)))
    if end_ > cut_to:
        out.append((max(start, cut_to), end_))
    return [(a, None if b == dt.date.max else b) for a, b in out if (b is None or a < b)]


def apply_corrections(
    tenures: pd.DataFrame,
    corrections: pd.DataFrame | None = None,
    *,
    live_edge: bool = True,
) -> pd.DataFrame:
    """Return *tenures* with curated (and, by default, live-edge) corrections applied.

    Curated rows are applied first, then the auto-generated Wikipedia
    live-edge rows (see :mod:`.live_edge`), which only describe changes
    newer than the fja05680 snapshot.
    """
    corr = load_corrections() if corrections is None else corrections
    if live_edge and corrections is None:
        from . import live_edge as le

        edge = le.load()
        if not edge.empty:
            corr = pd.concat([corr, edge[corr.columns]], ignore_index=True)
    if corr.empty:
        return tenures
    rows = [
        (str(r.ticker).upper(), r.start_date, (r.end_date if pd.notna(r.end_date) else None))
        for r in tenures.itertuples(index=False)
    ]
    for c in corr.itertuples(index=False):
        if c.action == "remove":
            new = []
            for t, s, e in rows:
                if t == c.ticker:
                    new += [(t, a, b) for a, b in _cut(s, e, c.start_date, c.end_date or dt.date.max)]
                else:
                    new.append((t, s, e))
            rows = new
        elif c.action == "add":
            if (c.ticker, c.start_date, c.end_date) not in rows:
                rows.append((c.ticker, c.start_date, c.end_date))
        else:
            raise ValueError(f"Unknown correction action {c.action!r}")
    out = pd.DataFrame(rows, columns=["ticker", "start_date", "end_date"])
    out["end_date"] = out["end_date"].astype(object).where(out["end_date"].notna(), None)
    log.info("Applied %s membership corrections", len(corr))
    return out.sort_values(["ticker", "start_date"]).reset_index(drop=True)
