"""Security master: resolve every S&P 500 membership tenure to an *entity*.

A membership tenure from fja05680 is ``(ticker, start_date, end_date)``. The
ticker is only a label: it may since have been renamed (ABC → COR), reused
by an unrelated company (STI: SunTrust → Solidion), or be a share class
(FOX, GOOG). Every downstream join must instead go through a stable
entity key. This module produces, per tenure:

* ``tiingo_key`` — the Tiingo permaTicker (or, for share classes Tiingo
  meta does not list separately, the plain ticker) whose price history
  covers the tenure. Prices *and* corporate actions are fetched by this key,
  so both come from the same entity.
* ``cik`` — the SEC entity, filled by :mod:`.cik_resolver`.

Resolution order for the entity, first hit wins:

1. **Curated override** (``data/entity_overrides.csv``) — renames where
   Tiingo files the entity under its new ticker (ABC → COR) and bankrupt
   names re-tickered with a Q suffix. Each row is documented.
2. **Tiingo meta candidates** whose latest ticker equals the member ticker.
   For reused tickers there are several; we fetch each and keep the one
   whose trading dates cover the tenure best.
3. **Plain ticker** — share classes (FOX, NWS, GOOG, UA) are priced by
   Tiingo under the ticker but not listed separately in fundamentals meta.

Every candidate is scored by *coverage*: the share of NYSE trading days in
the (post-2010) tenure window on which the entity has a bar. A chosen
entity below ``MIN_COVERAGE`` is recorded with ``status='low_coverage'``
rather than silently accepted.
"""

from __future__ import annotations

import datetime as dt
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ..constituents.changes import HISTORY_START, trading_days
from ..prices import tiingo
from . import tiingo_meta

log = logging.getLogger(__name__)

MIN_COVERAGE = 0.90
NO_SOURCE = "NONE"  # override marker: entity has no Tiingo series
OVERRIDES_PATH = Path(__file__).with_name("data") / "entity_overrides.csv"
FETCH_START = HISTORY_START


@dataclass
class SegmentResolution:
    ticker: str
    tenure_start: dt.date
    tenure_end: dt.date | None
    valid_from: dt.date
    valid_to: dt.date | None
    tiingo_key: str | None
    tiingo_name: str | None
    is_active: bool | None
    method: str
    coverage: float
    first_bar: dt.date | None
    last_bar: dt.date | None
    status: str
    cik_override: str | None
    note: str | None


def load_overrides() -> pd.DataFrame:
    cols = ["ticker", "valid_from", "valid_to", "tiingo_key", "cik", "note"]
    if not OVERRIDES_PATH.exists():
        return pd.DataFrame(columns=cols)
    df = pd.read_csv(OVERRIDES_PATH, dtype=str, comment="#").fillna("")
    df["ticker"] = df["ticker"].str.upper().str.strip()
    for c in ("valid_from", "valid_to"):
        df[c] = df[c].map(lambda v: dt.date.fromisoformat(v) if v else None)
    for c in ("tiingo_key", "cik"):
        df[c] = df[c].str.strip().replace("", None)
    if df["cik"].notna().any():
        df.loc[df["cik"].notna(), "cik"] = df.loc[df["cik"].notna(), "cik"].str.zfill(10)
    # Several rows may describe the same (ticker, range) — e.g. one pinning
    # the price series and another the CIK. Merge them so neither shadows
    # the other; conflicting non-null values are a curation error.
    key = df["ticker"] + "|" + df["valid_from"].map(str) + "|" + df["valid_to"].map(str)
    merged = []
    for _, g in df.groupby(key, sort=False):
        row = g.iloc[0].copy()
        for c in ("tiingo_key", "cik"):
            vals = g[c].dropna().unique()
            if len(vals) > 1:
                raise ValueError(f"Conflicting override {c} for {row['ticker']}: {list(vals)}")
            row[c] = vals[0] if len(vals) else None
        row["note"] = " | ".join(g["note"].astype(str))
        merged.append(row)
    return pd.DataFrame(merged, columns=cols).reset_index(drop=True)


def relevant_tenures(tenures: pd.DataFrame, *, start: dt.date = HISTORY_START) -> pd.DataFrame:
    """Tenures that overlap ``[start, today]``."""
    t = tenures.copy()
    t["ticker"] = t["ticker"].astype(str).str.upper()
    keep = t["end_date"].isna() | (pd.to_datetime(t["end_date"]) > pd.Timestamp(start))
    return t.loc[keep].reset_index(drop=True)


def split_into_segments(
    ticker: str, start: dt.date, end: dt.date | None, overrides: pd.DataFrame
) -> list[tuple[dt.date, dt.date | None, pd.Series | None]]:
    """Cut a tenure ``[start, end)`` at override boundaries.

    Returns ``(seg_start, seg_end, override_row_or_None)`` triples covering
    the tenure exactly once, clipped to ``HISTORY_START``.
    """
    lo = max(start, HISTORY_START)
    hi = end
    if hi is not None and hi <= lo:
        return []
    ov = overrides.loc[overrides["ticker"] == ticker]
    cuts = {lo}
    for r in ov.itertuples(index=False):
        for c in (r.valid_from, r.valid_to):
            if pd.notna(c) and c > lo and (hi is None or c < hi):
                cuts.add(c)
    points = sorted(cuts)
    bounds = list(zip(points, points[1:] + [hi], strict=True))
    out = []
    for a, b in bounds:
        match = None
        for _, r in ov.iterrows():
            f = r["valid_from"] if pd.notna(r["valid_from"]) else dt.date.min
            t = r["valid_to"] if pd.notna(r["valid_to"]) else dt.date.max
            if f <= a and (b or dt.date.max) <= t:
                match = r
                break
        out.append((a, b, match))
    return out


def _window_days(start: dt.date, end: dt.date | None, cal: list[dt.date]) -> list[dt.date]:
    lo = max(start, HISTORY_START)
    hi = end or dt.date.max
    return [d for d in cal if lo <= d < hi]


def _coverage(bars: pd.DataFrame, window: list[dt.date]) -> float:
    if not window:
        return 0.0
    have = set(bars["date"]) if not bars.empty else set()
    return sum(1 for d in window if d in have) / len(window)


def _fetch(key: str, end: dt.date) -> pd.DataFrame:
    try:
        return tiingo.fetch_entity_history(key, start=FETCH_START, end=end)
    except Exception as e:  # network / API errors are recorded, not fatal
        log.warning("Tiingo fetch failed for %s: %s", key, e)
        return pd.DataFrame(columns=tiingo.ENTITY_COLUMNS)


def resolve_entities(
    tenures: pd.DataFrame,
    *,
    end: dt.date | None = None,
    max_workers: int = 6,
) -> pd.DataFrame:
    """Resolve each post-2010 tenure segment to a Tiingo entity."""
    end = end or dt.date.today()
    ten = relevant_tenures(tenures)
    overrides = load_overrides()
    meta = tiingo_meta.load_meta()
    meta_by_key = meta.drop_duplicates("permaTicker").set_index("permaTicker")

    # 1. Enumerate segments and their candidate keys.
    segs = []
    for r in ten.itertuples(index=False):
        end_d = r.end_date if pd.notna(r.end_date) else None
        for a, b, ov in split_into_segments(r.ticker, r.start_date, end_d, overrides):
            keys: list[tuple[str, str]] = []
            if ov is not None and ov["tiingo_key"] == NO_SOURCE:
                pass  # curated: no Tiingo series exists for this entity
            elif ov is not None and pd.notna(ov["tiingo_key"]):
                keys.append((ov["tiingo_key"], "override"))
            else:
                for perma in tiingo_meta.candidates_for(r.ticker)["permaTicker"]:
                    keys.append((perma, "meta"))
                keys.append((r.ticker.replace(".", "-"), "ticker_direct"))
            segs.append((r, end_d, a, b, ov, keys))

    # 2. Fetch every distinct key once (cached on disk).
    all_keys = sorted({k for *_, keys in segs for k, _ in keys})
    log.info("security_master: %s segments, %s Tiingo series", len(segs), len(all_keys))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        bars = dict(zip(all_keys, ex.map(lambda k: _fetch(k, end), all_keys), strict=True))

    # 3. Score candidates by coverage of the segment window.
    cal = trading_days(HISTORY_START, end)
    rows: list[SegmentResolution] = []
    for r, end_d, a, b, ov, keys in segs:
        window = _window_days(a, b, cal)
        best: tuple[float, str, str] | None = None
        for key, method in keys:
            cov = _coverage(bars[key], window)
            if best is None or cov > best[0] + 1e-9:
                best = (cov, key, method)
        cov, key, method = best if best else (0.0, None, "none")
        bk = bars.get(key) if key else None
        name = active = None
        if key in meta_by_key.index:
            m = meta_by_key.loc[key]
            name, active = m["name"], bool(m["isActive"])
        if not window:
            status = "empty_window"
        elif not keys:
            status = "no_source"
        elif cov >= MIN_COVERAGE:
            status = "ok"
        else:
            status = "low_coverage" if cov > 0 else "unresolved"
        rows.append(
            SegmentResolution(
                ticker=r.ticker,
                tenure_start=r.start_date,
                tenure_end=end_d,
                valid_from=a,
                valid_to=b,
                tiingo_key=key if cov > 0 else None,
                tiingo_name=name,
                is_active=active,
                method=method if cov > 0 else "none",
                coverage=round(cov, 4),
                first_bar=(bk["date"].min() if bk is not None and not bk.empty else None),
                last_bar=(bk["date"].max() if bk is not None and not bk.empty else None),
                status=status,
                cik_override=(ov["cik"] if ov is not None and pd.notna(ov["cik"]) else None),
                note=(ov["note"] if ov is not None else None),
            )
        )
    out = pd.DataFrame([x.__dict__ for x in rows])
    log.info("security_master: %s", out["status"].value_counts().to_dict())
    return out


def build_security_master(tenures: pd.DataFrame, *, end: dt.date | None = None) -> pd.DataFrame:
    """Run entity → CIK → ticker-label resolution and persist the result."""
    import pyarrow as pa

    from ..constituents.wikipedia_history import load_snapshots
    from ..store import schemas, writer
    from .cik_resolver import resolve_ciks
    from .ticker_labels import label_parts

    wiki = load_snapshots()
    segs = resolve_entities(tenures, end=end)
    parts = resolve_ciks(segs, wiki)
    labeled = label_parts(parts, wiki, relevant_tenures(tenures))

    df = pd.DataFrame(
        {
            "ticker": labeled["ticker"],
            "ticker_pit": labeled["ticker_pit"],
            "valid_from": labeled["valid_from"],
            "valid_to": labeled["valid_to"].where(labeled["valid_to"].notna(), None),
            "cik": labeled["cik"],
            "perma_ticker": labeled["tiingo_key"],
            "vendor_name": labeled["tiingo_name"],
            "price_status": labeled["price_status"],
            "price_coverage": labeled["price_coverage"],
            "entity_method": labeled["entity_method"],
            "cik_method": labeled["cik_method"],
            "label_source": labeled["label_source"],
            "flags": labeled["flags"],
            "ingestion_date": dt.date.today(),
        }
    ).sort_values(["ticker", "valid_from"])
    table = pa.Table.from_pandas(df, schema=schemas.SECURITY_MASTER, preserve_index=False)
    writer.write_parquet(table, writer.security_master_path())
    log.info("security_master: %s rows written", table.num_rows)
    return df


def load_security_master() -> pd.DataFrame:
    from ..store import writer

    return pd.read_parquet(writer.security_master_path())
