"""Assign a point-in-time SEC CIK to every security-master segment.

Evidence, strongest first:

1. **Curated override** (``entity_overrides.csv`` ``cik`` column).
2. **Wikipedia snapshots** (quarterly, CIK column since mid-2014): the CIK
   listed next to the ticker *while the segment was live*. If the CIK changes
   inside a segment (holding-company reorg), the segment is split at the new
   CIK's first periodic filing.
3. **EDGAR name search** for periods with no Wikipedia CIK (pre-2014 or
   predecessor periods): fuzzy-match the Wikipedia security name against
   every EDGAR entity name, keep candidates whose periodic filings span the
   period.
4. **SEC current ticker map**, only for the active Tiingo entity.

Every CIK is then *validated*: it must have filed periodic reports spanning
the part (± ~6 months). Failures and cross-source disagreements are written
to ``flags`` rather than dropped, so the residual uncertainty is visible.

Cross-check against the price entity: when the Tiingo entity is active, its
current ticker maps (via SEC) to a CIK. If that CIK differs from the
segment's CIK *and* was already filing during the segment, the price series
belongs to a different company — flagged ``entity_mismatch``. (Predecessor
CIKs, which stopped filing when the successor started, are expected and not
flagged.)
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass, field

import pandas as pd
from rapidfuzz import fuzz

from . import edgar_entities as ee
from .cik_map import load_ticker_to_cik
from .tiingo_meta import load_meta

log = logging.getLogger(__name__)

NEAR_DAYS = 100
SLACK = dt.timedelta(days=200)


@dataclass
class Part:
    valid_from: dt.date
    valid_to: dt.date | None
    cik: str | None
    method: str
    flags: list[str] = field(default_factory=list)


def _sec_cik(ticker: str, sec: dict[str, str]) -> str | None:
    t = ticker.upper()
    for v in (t, t.replace(".", "-"), t.replace("-", ".")):
        if v in sec:
            return sec[v]
    return None


def _name_score(a: list[str], b: list[str]) -> float:
    best = 0.0
    for x in a:
        nx = ee.normalize_name(x)
        for y in b:
            best = max(best, fuzz.token_sort_ratio(nx, ee.normalize_name(y)))
    return best


def _subset_name_score(name: str, others: list[str]) -> float:
    """token_set_ratio: 'Uber' vs 'Uber Technologies' scores 100, unrelated names don't."""
    n = ee.normalize_name(name)
    return max((fuzz.token_set_ratio(n, ee.normalize_name(o)) for o in others), default=0.0)


def _search_by_names(names: list[str], start: dt.date, end: dt.date | None) -> tuple[str | None, str]:
    """EDGAR name search restricted to CIKs filing during [start, end)."""
    best: tuple[float, str] | None = None
    for n in dict.fromkeys(names):  # dedupe, keep order
        for cik, _ename, score in ee.search_name(n):
            if ee.covers(cik, start, end) and (best is None or score > best[0]):
                best = (score, cik)
    if best is None:
        return None, "name_search_failed"
    return best[1], f"name_search({best[0]:.0f})"


def _wiki_rows(wiki: pd.DataFrame, ticker: str, a: dt.date, b: dt.date | None) -> pd.DataFrame:
    w = wiki.loc[wiki["ticker"] == ticker]
    hi = b or dt.date.max
    inside = w.loc[(w["snapshot_date"] >= a) & (w["snapshot_date"] < hi)]
    if not inside.empty:
        return inside.sort_values("snapshot_date")
    near = w.loc[
        (w["snapshot_date"] >= a - dt.timedelta(days=NEAR_DAYS))
        & (w["snapshot_date"] < (hi if b is None else b + dt.timedelta(days=NEAR_DAYS)))
    ]
    return near.sort_values("snapshot_date")


def _cik_runs(w: pd.DataFrame) -> list[tuple[str, dt.date, dt.date]]:
    """Collapse dated CIK observations into runs, dropping 1-snapshot blips (A,X,A).

    Observations whose CIK's EDGAR name history doesn't resemble the security
    name Wikipedia shows on that row are discarded first (Wikipedia once
    listed Hanesbrands with Avery Dennison's CIK).
    """
    raw = [(r.snapshot_date, r.cik, r.security) for r in w.itertuples() if pd.notna(r.cik)]
    obs = [(d, c) for d, c, sec in raw if _subset_name_score(sec, ee.entity_names(c)) >= 50]
    if raw and not obs:  # never strand a member without evidence; validation flags it
        obs = [(d, c) for d, c, _ in raw]
    cleaned = []
    for i, (d, c) in enumerate(obs):
        prev = obs[i - 1][1] if i > 0 else None
        nxt = obs[i + 1][1] if i + 1 < len(obs) else None
        if prev is not None and prev == nxt and c != prev:
            continue
        cleaned.append((d, c))
    runs: list[list] = []
    for d, c in cleaned:
        if runs and runs[-1][0] == c:
            runs[-1][2] = d
        else:
            runs.append([c, d, d])
    return [tuple(r) for r in runs]


RUN_SLACK = dt.timedelta(days=120)


def _valid_runs(runs: list[tuple[str, dt.date, dt.date]]) -> list[tuple[str, dt.date, dt.date]]:
    """Drop Wikipedia CIK runs that EDGAR contradicts.

    A run is implausible when its CIK never filed periodic reports, or had
    stopped filing well before (or only started well after) the snapshots
    that list it. Wikipedia occasionally carries a subsidiary's or a merger
    partner's CIK. If *every* run is implausible (e.g. banks that report to
    the FDIC, not the SEC), keep the originals and let validation flag them.
    """
    good = []
    for cik, first_seen, last_seen in runs:
        f, last = ee.periodic_span(cik)
        if f is None:
            continue
        if last < first_seen - RUN_SLACK or f > last_seen + RUN_SLACK:
            continue
        good.append((cik, first_seen, last_seen))
    # Merge neighbours that became adjacent duplicates after dropping.
    merged: list[list] = []
    for c, f, last in good:
        if merged and merged[-1][0] == c:
            merged[-1][2] = last
        else:
            merged.append([c, f, last])
    return [tuple(m) for m in merged] if merged else runs


def _parts_for_segment(seg, wiki: pd.DataFrame, sec: dict[str, str]) -> tuple[list[Part], list[str]]:
    a, b = seg.valid_from, (seg.valid_to if pd.notna(seg.valid_to) else None)
    w = _wiki_rows(wiki, seg.ticker, a, b)
    names = list(w["security"].dropna().unique())
    if isinstance(seg.tiingo_name, str) and seg.tiingo_name:
        names_plus = names + [seg.tiingo_name]
    else:
        names_plus = names

    if isinstance(seg.cik_override, str) and seg.cik_override:
        return [Part(a, b, seg.cik_override, "override")], names

    runs = _valid_runs(_cik_runs(w))
    parts: list[Part] = []
    if runs:
        bounds = [a]
        for (_c_prev, _f, _last_prev), (c_next, first_next, _l) in zip(runs, runs[1:], strict=False):
            # Wikipedia's CIK column often lags a reorganization by months or
            # years; the successor's first periodic report is the better cut.
            fp, _ = ee.periodic_span(c_next)
            cut = fp if fp is not None and bounds[-1] < fp <= first_next else first_next
            bounds.append(cut)
        bounds.append(b)
        for (c, _f, _l), lo, hi in zip(runs, bounds[:-1], bounds[1:], strict=True):
            parts.append(Part(lo, hi, c, "wiki"))
    else:
        # No Wikipedia CIK (pre-2014 segment): candidates from SEC + name search.
        cands: list[tuple[str, str]] = []
        if seg.is_active and isinstance(seg.tiingo_key, str):
            meta = load_meta().drop_duplicates("permaTicker").set_index("permaTicker")
            if seg.tiingo_key in meta.index:
                c = _sec_cik(meta.loc[seg.tiingo_key, "ticker"], sec)
                if c:
                    cands.append((c, "sec_entity_ticker"))
        if b is None:
            c = _sec_cik(seg.ticker, sec)
            if c:
                cands.append((c, "sec_member_ticker"))
        chosen = None
        for c, m in cands:
            if ee.covers(c, a, b) and (not names or _name_score(names, ee.entity_names(c)) >= 70):
                chosen = (c, m)
                break
        if chosen is None:
            c, m = _search_by_names(names_plus, a, b)
            chosen = (c, m)
        parts.append(Part(a, b, chosen[0], chosen[1]))

    # Validate each part; patch predecessor periods via name search.
    final: list[Part] = []
    for p in parts:
        if p.cik is None:
            p.flags.append("no_cik")
            final.append(p)
            continue
        first, last = ee.periodic_span(p.cik)
        if first is None:
            p.flags.append("cik_no_periodic_filings")
            final.append(p)
            continue
        if first > p.valid_from + SLACK:
            # CIK did not exist yet for the start of this part → predecessor.
            pred, m = _search_by_names(names_plus, p.valid_from, first)
            if pred and pred != p.cik:
                final.append(Part(p.valid_from, first, pred, f"predecessor:{m}"))
                p.valid_from = first
            else:
                p.flags.append("cik_starts_late")
        end = p.valid_to or dt.date.today()
        if last < min(end, ee.submissions_asof(p.cik)) - dt.timedelta(days=400):
            p.flags.append("cik_stops_early")
        final.append(p)
    return final, names


def resolve_ciks(segments: pd.DataFrame, wiki: pd.DataFrame) -> pd.DataFrame:
    """Return one row per (segment part) with CIK, method and validation flags."""
    sec = load_ticker_to_cik()
    meta = load_meta().drop_duplicates("permaTicker").set_index("permaTicker")
    rows = []
    for i, seg in enumerate(segments.itertuples(index=False), 1):
        if i % 100 == 0:
            log.info("cik_resolver: %s/%s segments", i, len(segments))
        try:
            parts, names = _parts_for_segment(seg, wiki, sec)
        except Exception as e:  # keep going; surface the failure
            log.warning("cik_resolver failed for %s %s: %s", seg.ticker, seg.valid_from, e)
            parts, names = [Part(seg.valid_from, seg.valid_to, None, "error", [f"error:{e}"])], []

        # Price-entity cross-check.
        ecik = None
        if seg.is_active and isinstance(seg.tiingo_key, str) and seg.tiingo_key in meta.index:
            ecik = _sec_cik(meta.loc[seg.tiingo_key, "ticker"], sec)
        for p in parts:
            flags = list(p.flags)
            # Curated price entities were verified by hand; skip the heuristic.
            if ecik and p.cik and ecik != p.cik and seg.method != "override":
                efirst, _ = ee.periodic_span(ecik)
                if efirst is not None and efirst < (p.valid_to or dt.date.today()) - SLACK:
                    flags.append(f"entity_mismatch(price_entity_cik={ecik})")
            if (not ecik) and isinstance(seg.tiingo_name, str) and names and _name_score([seg.tiingo_name], names) < 60:
                flags.append("tiingo_name_mismatch")
            rows.append(
                {
                    "ticker": seg.ticker,
                    "valid_from": p.valid_from,
                    "valid_to": p.valid_to,
                    "tiingo_key": seg.tiingo_key,
                    "tiingo_name": seg.tiingo_name,
                    "entity_method": seg.method,
                    "price_status": seg.status,
                    "price_coverage": seg.coverage,
                    "cik": p.cik,
                    "cik_method": p.method,
                    "wiki_name": names[-1] if names else None,
                    "edgar_name": (ee.entity_names(p.cik)[0] if p.cik and ee.entity_names(p.cik) else None),
                    "flags": ";".join(flags) or None,
                }
            )
    return pd.DataFrame(rows)
