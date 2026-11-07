"""Point-in-time ticker labels for security-master parts.

fja05680 labels each tenure with the company's *latest* ticker: Priceline's
2010-2018 membership appears as ``BKNG`` although it traded as ``PCLN``
until 2018-02-27. A PIT dataset must answer "what was the ticker on date T",
so we recover the as-of label from the dated Wikipedia snapshots:

* On each quarterly snapshot inside a part, the part's label is the fja
  ticker if Wikipedia lists it; otherwise the unique *unmatched* Wikipedia
  row (not itself an fja member that day) whose CIK equals the part's CIK,
  or — before Wikipedia carried CIKs — whose security name matches one of
  the CIK's EDGAR names.
* Where consecutive snapshots disagree, the exact switch date is found by
  bisecting the article's revisions (``find_label_change``).
"""

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz

from ..constituents.wikipedia_history import find_label_change
from . import edgar_entities as ee

log = logging.getLogger(__name__)


def _same(a: str, b: str) -> bool:
    # Early Wikipedia revisions spell class shares without the dot (BRKB, BFB).
    return a.replace(".", "") == b.replace(".", "")


def _members_on(tenures: pd.DataFrame, d: dt.date) -> set[str]:
    m = (tenures["start_date"] <= d) & (tenures["end_date"].isna() | (tenures["end_date"] > d))
    return set(tenures.loc[m, "ticker"])


CURATED_PATH = Path(__file__).with_name("data") / "ticker_labels.csv"


def load_curated_labels() -> dict[str, list[tuple[dt.date, str]]]:
    if not CURATED_PATH.exists():
        return {}
    df = pd.read_csv(CURATED_PATH, comment="#", dtype=str)
    out: dict[str, list[tuple[dt.date, str]]] = {}
    for r in df.itertuples(index=False):
        out.setdefault(r.ticker.upper(), []).append((dt.date.fromisoformat(r.valid_from), r.label))
    return {k: sorted(v) for k, v in out.items()}


def _curated_rows(p, seq: list[tuple[dt.date, str]]) -> list[dict]:
    a = p.valid_from
    b = p.valid_to if pd.notna(p.valid_to) else None
    rows = []
    for i, (start, lab) in enumerate(seq):
        stop = seq[i + 1][0] if i + 1 < len(seq) else None
        lo = max(a, start)
        hi = b if stop is None else (stop if b is None else min(b, stop))
        if hi is not None and hi <= lo:
            continue
        rows.append({**p._asdict(), "valid_from": lo, "valid_to": hi, "ticker_pit": lab, "label_source": "curated"})
    return rows


def label_parts(parts: pd.DataFrame, wiki: pd.DataFrame, tenures: pd.DataFrame) -> pd.DataFrame:
    """Expand parts into (part, label) intervals. Adds ``ticker_pit`` + ``label_source``."""
    curated = load_curated_labels()
    ten = tenures.copy()
    ten["end_date"] = ten["end_date"].where(ten["end_date"].notna(), None)
    snaps = dict(tuple(wiki.groupby("snapshot_date")))
    snap_dates = sorted(snaps)
    members_cache: dict[dt.date, set[str]] = {}

    out = []
    for p in parts.itertuples(index=False):
        if p.ticker in curated:
            out += _curated_rows(p, curated[p.ticker])
            continue
        a = p.valid_from
        b = p.valid_to if pd.notna(p.valid_to) else None
        names = ee.entity_names(p.cik) if isinstance(p.cik, str) else []
        obs: list[tuple[dt.date, str]] = []
        for sd in snap_dates:
            if sd < a or (b is not None and sd >= b):
                continue
            g = snaps[sd]
            if any(_same(t, p.ticker) for t in g["ticker"]):
                obs.append((sd, p.ticker))
                continue
            if sd not in members_cache:
                members_cache[sd] = _members_on(ten, sd)
            mem = members_cache[sd]
            cand = g.loc[~g["ticker"].map(lambda t, mem=mem: any(_same(t, m) for m in mem))]
            hit = cand.loc[cand["cik"] == p.cik] if isinstance(p.cik, str) else cand.iloc[0:0]
            if hit.empty and names:
                score = cand["security"].map(
                    lambda s, names=names: max(
                        fuzz.token_sort_ratio(ee.normalize_name(s), ee.normalize_name(n)) for n in names
                    )
                )
                hit = cand.loc[score >= 88]
            if len(hit) == 1:
                obs.append((sd, hit["ticker"].iloc[0]))

        # Collapse to runs and locate exact switch dates.
        runs: list[list] = []
        for sd, lab in obs:
            if runs and _same(runs[-1][0], lab):
                runs[-1][2] = sd
            else:
                runs.append([lab, sd, sd])
        if not runs:
            out.append({**p._asdict(), "ticker_pit": p.ticker, "label_source": "fja_default"})
            continue
        cuts = [a]
        for (old, _f, last_old), (new, first_new, _l) in zip(runs, runs[1:], strict=False):
            when = None
            try:
                when = find_label_change(old, new, last_old, first_new)
            except Exception as e:  # network trouble → fall back to snapshot date
                log.warning("label bisect %s→%s failed: %s", old, new, e)
            cuts.append(when or first_new)
        cuts.append(b)
        for (lab, _f, _l), lo, hi in zip(runs, cuts[:-1], cuts[1:], strict=True):
            row = {**p._asdict(), "valid_from": lo, "valid_to": hi}
            row["ticker_pit"] = p.ticker if _same(lab, p.ticker) else lab
            row["label_source"] = "fja" if _same(lab, p.ticker) else "wikipedia"
            out.append(row)
    return pd.DataFrame(out)
