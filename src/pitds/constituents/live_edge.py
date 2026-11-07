"""Live-edge membership: index changes newer than the fja05680 snapshot.

fja05680 is refreshed roughly monthly and can lag S&P announcements by
weeks to months. To keep membership point-in-time correct up to *today*,
we diff fja's current membership against the latest revision of
Wikipedia's "List of S&P 500 companies" and date each difference by
bisecting the article's revisions (edit date ≈ effective date, ±1 day).

Rules:

* Only differences fja does not already know about are emitted; once fja
  catches up the diff disappears and fja's own dates take over.
* A remove + add pair carrying the *same CIK* is a ticker rename, not a
  membership change — it is skipped (labels are the security master's job).
* Output is written to ``data/processed/live_edge_corrections.parquet`` and
  applied by :mod:`.corrections` alongside the curated CSV, tagged
  ``source='wikipedia_live_edge'`` so it is always distinguishable.
"""

from __future__ import annotations

import datetime as dt
import logging

import pandas as pd

from ..store import writer
from .wikipedia_history import find_presence_change, latest_revision

log = logging.getLogger(__name__)

LOOKBACK_DAYS = 120


def path():
    return writer.settings.processed_dir / "live_edge_corrections.parquet"


def _open_members(tenures: pd.DataFrame, on: dt.date) -> set[str]:
    m = (tenures["start_date"] <= on) & (tenures["end_date"].isna() | (tenures["end_date"] > on))
    return set(tenures.loc[m, "ticker"].astype(str).str.upper())


def compute_live_edge(tenures: pd.DataFrame, sm: pd.DataFrame | None, *, today: dt.date | None = None) -> pd.DataFrame:
    """Return correction rows (action, ticker, start_date, end_date, note)."""
    today = today or dt.date.today()
    revid, ts, wiki = latest_revision()
    w_tickers = set(wiki["ticker"])
    w_cik = dict(zip(wiki["ticker"], wiki["cik"], strict=False))

    fja = _open_members(tenures, today)
    # Compare on as-of labels: fja ticker → current ticker_pit (+ CIK).
    label, cik_of = {}, {}
    for t in fja:
        lab, cik = t, None
        if sm is not None:
            m = sm.loc[(sm["ticker"] == t) & sm["valid_to"].isna()]
            if not m.empty:
                lab, cik = m.iloc[0]["ticker_pit"], m.iloc[0]["cik"]
        label[lab] = t
        cik_of[lab] = cik
    f_labels = set(label)

    adds = sorted(w_tickers - f_labels)
    removes = sorted(f_labels - w_tickers)
    # Same-CIK pairs are renames.
    renamed = {
        (r, a) for r in removes for a in adds if cik_of.get(r) and cik_of.get(r) == w_cik.get(a)
    }
    adds = [a for a in adds if a not in {x[1] for x in renamed}]
    removes = [r for r in removes if r not in {x[0] for x in renamed}]

    horizon = max(
        pd.Series(tenures["start_date"]).max(),
        pd.Series(tenures["end_date"]).dropna().max(),
    )
    after = min(horizon, today) - dt.timedelta(days=7)
    after = max(after, today - dt.timedelta(days=LOOKBACK_DAYS))

    rows = []
    for a in adds:
        d = find_presence_change(a, after, today, now_present=True)
        rows.append(
            {
                "action": "add",
                "ticker": a,
                "start_date": d or today,
                "end_date": None,
                "note": f"wikipedia_live_edge rev {revid} ({ts}); cik={w_cik.get(a)}; date={'bisected' if d else 'UNDATED'}",
            }
        )
    for r in removes:
        d = find_presence_change(r, after, today, now_present=False)
        rows.append(
            {
                "action": "remove",
                "ticker": label[r],
                "start_date": d or today,
                "end_date": None,
                "note": f"wikipedia_live_edge rev {revid} ({ts}); label={r}; date={'bisected' if d else 'UNDATED'}",
            }
        )
    out = pd.DataFrame(rows, columns=["action", "ticker", "start_date", "end_date", "note"])
    log.info("live edge: %s adds, %s removes, %s renames skipped", len(adds), len(removes), len(renamed))
    return out


def refresh(tenures: pd.DataFrame, sm: pd.DataFrame | None) -> pd.DataFrame:
    df = compute_live_edge(tenures, sm)
    df.to_parquet(path(), index=False)
    return df


def load() -> pd.DataFrame:
    p = path()
    if not p.exists():
        return pd.DataFrame(columns=["action", "ticker", "start_date", "end_date", "note"])
    df = pd.read_parquet(p)
    for c in ("start_date", "end_date"):
        df[c] = df[c].map(lambda v: v if (v is None or isinstance(v, dt.date)) else pd.Timestamp(v).date() if pd.notna(v) else None)
    return df
