"""Entity-keyed price and corporate-action build from the security master.

Replaces the ticker-keyed cascade for the S&P 500 universe. For every
security-master row with a vendor series (``perma_ticker``):

* bars inside the row's ``[valid_from, valid_to)`` window are emitted with
  the row's as-of ticker (``ticker_pit``) and CIK;
* bars *outside* any membership window are kept as lookback history only
  across genuine index add/remove edges (``valid_from`` equals the tenure
  start / ``valid_to`` equals the tenure end), never across a curated
  entity split — on the far side of such a split the series belongs to a
  different company (e.g. Tiingo's CCEP series before 2010-10-04 is the old
  Coca-Cola Enterprises).

Dividends and splits come from the same Tiingo response (``divCash``,
``splitFactor``), so prices and the action log always describe the same
entity. Rows on (ticker, date) collisions prefer in-membership bars.
"""

from __future__ import annotations

import datetime as dt
import logging
import uuid
from pathlib import Path

import pandas as pd
import pyarrow as pa

from ..constituents.changes import HISTORY_START
from ..store import schemas, writer
from . import tiingo

log = logging.getLogger(__name__)

PRICE_STATUSES = ("ok", "low_coverage")


def _extension_windows(sm: pd.DataFrame, tenures: pd.DataFrame) -> pd.DataFrame:
    """Per security-master row, the date window its labels may cover.

    * Inside membership: ``[valid_from, valid_to)``.
    * **Gaps** between two rows of the same vendor series (a company removed
      and later re-added — T-Mobile 2013→2019) are filled and labelled with
      the *later* row, so every series is continuous and naive return
      calculations never span a multi-year hole.
    * **Lookback** before the first row / after the last row of a series is
      kept only across a genuine index add/remove edge, and never when the
      same CIK continues under another membership row on that side (a
      renamed company re-priced under its new ticker: SYMC → NLOK → GEN) —
      otherwise the old ticker would be emitted for dates it didn't trade.
    """
    ten_starts = {(t, max(HISTORY_START, s)) for t, s in zip(tenures["ticker"], tenures["start_date"], strict=False)}
    ten_ends = set(zip(tenures["ticker"], tenures["end_date"], strict=False))
    rows = sm.loc[sm["perma_ticker"].notna() & sm["price_status"].isin(PRICE_STATUSES)].copy()
    rows = rows.sort_values(["perma_ticker", "valid_from"])
    rows["win_from"] = rows["valid_from"]
    rows["win_to"] = rows["valid_to"]

    cik_rows = sm.loc[sm["cik"].notna(), ["cik", "valid_from", "valid_to"]]
    cik_first = cik_rows.groupby("cik")["valid_from"].min().to_dict()
    cik_open = cik_rows.groupby("cik")["valid_to"].apply(lambda s: s.isna().any()).to_dict()
    cik_last = cik_rows.dropna(subset=["valid_to"]).groupby("cik")["valid_to"].max().to_dict()

    for _key, g in rows.groupby("perma_ticker"):
        idx = list(g.index)
        # Fill interior gaps with the later row.
        for prev, nxt in zip(idx, idx[1:], strict=False):
            pv_to = rows.loc[prev, "valid_to"]
            if pd.notna(pv_to) and pv_to < rows.loc[nxt, "valid_from"]:
                rows.loc[nxt, "win_from"] = pv_to
        first, last = idx[0], idx[-1]
        f = rows.loc[first]
        starts_at_tenure = (f["ticker"], max(HISTORY_START, f["valid_from"])) in ten_starts
        cik_earlier = isinstance(f["cik"], str) and cik_first.get(f["cik"], f["valid_from"]) < f["valid_from"]
        if starts_at_tenure and not cik_earlier:
            rows.loc[first, "win_from"] = HISTORY_START
        lrow = rows.loc[last]
        ends_at_tenure = pd.isna(lrow["valid_to"]) or (lrow["ticker"], lrow["valid_to"]) in ten_ends
        cik_later = isinstance(lrow["cik"], str) and pd.notna(lrow["valid_to"]) and (
            cik_open.get(lrow["cik"], False) or cik_last.get(lrow["cik"], lrow["valid_to"]) > lrow["valid_to"]
        )
        if ends_at_tenure and not cik_later:
            rows.loc[last, "win_to"] = None
    return rows


ACTION_CORRECTIONS = Path(__file__).resolve().parents[1] / "corporate_actions" / "data" / "action_corrections.csv"


def _apply_action_corrections(actions: pd.DataFrame, prices: pd.DataFrame, today: dt.date) -> pd.DataFrame:
    """Apply curated add/drop fixes to the vendor action log."""
    if not ACTION_CORRECTIONS.exists():
        return actions
    corr = pd.read_csv(ACTION_CORRECTIONS, comment="#", dtype=str).fillna("")
    for c in corr.itertuples(index=False):
        ex = dt.date.fromisoformat(c.ex_date)
        if c.op == "drop":
            m = (actions["perma_ticker"] == c.perma_ticker) & (actions["ex_date"] == ex) & (actions["action_type"] == c.action_type)
            actions = actions.loc[~m]
        elif c.op == "add":
            lab = prices.loc[(prices["perma_ticker"] == c.perma_ticker) & (prices["date"] == ex)]
            if lab.empty:
                log.warning("action correction for %s %s: no price row to label it", c.perma_ticker, ex)
                continue
            actions = pd.concat(
                [
                    actions,
                    pd.DataFrame(
                        [
                            {
                                "action_id": str(uuid.uuid4()),
                                "cik": lab["cik"].iloc[0],
                                "ticker": lab["ticker"].iloc[0],
                                "ex_date": ex,
                                "action_type": c.action_type,
                                "ratio": float(c.ratio) if c.ratio else None,
                                "amount": float(c.amount) if c.amount else None,
                                "spinoff_child_cik": None,
                                "source": "manual_curated",
                                "ingestion_date": today,
                                "perma_ticker": c.perma_ticker,
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )
    return actions


def build_tables(
    sm: pd.DataFrame,
    tenures: pd.DataFrame,
    *,
    end: dt.date | None = None,
) -> tuple[pa.Table, pa.Table]:
    """Return (prices, corporate_actions) Arrow tables built from vendor series."""
    end = end or dt.date.today()
    today = dt.date.today()
    win = _extension_windows(sm, tenures)
    price_frames, action_frames, series_actions = [], [], []
    for key, g in win.groupby("perma_ticker"):
        bars = tiingo.fetch_entity_history(key, start=HISTORY_START, end=end)
        if bars.empty:
            continue
        bars = bars.copy()
        # Actions are taken from the *whole* vendor series, not only emitted
        # bars: a split falling between two emitted stretches of the same
        # series still has to adjust the earlier stretch.
        sa = bars.loc[
            (pd.to_numeric(bars["divCash"], errors="coerce").fillna(0) > 0)
            | ((pd.to_numeric(bars["splitFactor"], errors="coerce").fillna(1) - 1).abs() > 1e-9),
            ["date", "divCash", "splitFactor"],
        ].copy()
        sa["perma_ticker"] = key
        series_actions.append(sa)
        for r in g.itertuples():
            lo = r.win_from
            hi = r.win_to if pd.notna(r.win_to) else dt.date.max
            m = (bars["date"] >= lo) & (bars["date"] < hi)
            if not m.any():
                continue
            b = bars.loc[m].copy()
            b["ticker"] = r.ticker_pit
            b["cik"] = r.cik
            b["perma_ticker"] = key
            in_mem_hi = r.valid_to if pd.notna(r.valid_to) else dt.date.max
            b["in_membership"] = (b["date"] >= r.valid_from) & (b["date"] < in_mem_hi)
            price_frames.append(b)

    if not price_frames:
        raise RuntimeError("No vendor bars available — run the security-master build first")
    allb = pd.concat(price_frames, ignore_index=True)
    # Lookback bars carry the label of the nearest membership row, but before
    # joining the index a company may have traded under another symbol while
    # *its future ticker belonged to a different member* (AGL Resources only
    # took GAS from Nicor in 2011-12). Drop lookback bars whose (ticker, date)
    # is another CIK's membership day — that label is provably wrong.
    import duckdb

    con = duckdb.connect()
    con.register("b", allb[["ticker", "date", "cik", "in_membership"]].reset_index())
    con.register("sm", sm[["ticker_pit", "cik", "valid_from", "valid_to"]])
    wrong = con.sql(
        """
        SELECT DISTINCT b.index FROM b JOIN sm
          ON sm.ticker_pit = b.ticker
         AND b.date >= sm.valid_from
         AND b.date < COALESCE(sm.valid_to, DATE '9999-12-31')
        WHERE NOT b.in_membership AND sm.cik IS DISTINCT FROM b.cik
        """
    ).df()["index"]
    con.close()
    if len(wrong):
        log.info("entity_loader: dropped %s lookback bars mislabelled with another member's ticker", len(wrong))
        allb = allb.drop(index=wrong)
    # (ticker, date) collisions: prefer in-membership rows.
    allb = allb.sort_values(["ticker", "date", "in_membership"], ascending=[True, True, False])
    dup = allb.duplicated(["ticker", "date"], keep="first")
    if dup.any():
        log.info("entity_loader: dropped %s colliding out-of-membership bars", int(dup.sum()))
    allb = allb.loc[~dup]
    # The same vendor bar can be emitted twice when two security-master rows
    # share a perma_ticker and windows touch; dedupe on (perma, date).
    allb = allb.drop_duplicates(["perma_ticker", "date"], keep="first")

    prices = pd.DataFrame(
        {
            "date": allb["date"],
            "ticker": allb["ticker"],
            "cik": allb["cik"].astype("string"),
            "figi": pd.Series([None] * len(allb), dtype="string"),
            "open": pd.to_numeric(allb["open"], errors="coerce"),
            "high": pd.to_numeric(allb["high"], errors="coerce"),
            "low": pd.to_numeric(allb["low"], errors="coerce"),
            "close": pd.to_numeric(allb["close"], errors="coerce"),
            "volume": pd.to_numeric(allb["volume"], errors="coerce").round().astype("Int64"),
            "source": "tiingo",
            "ingestion_date": today,
            "perma_ticker": allb["perma_ticker"],
        }
    )
    prices = prices.loc[prices["close"].notna()].sort_values(["date", "ticker"])

    # Label each series action with the ticker/CIK of the nearest emitted bar
    # of the same series (previous bar preferred, else next).
    sa = pd.concat(series_actions, ignore_index=True)
    lab = prices[["perma_ticker", "date", "ticker", "cik"]]
    sa["date_ts"] = pd.to_datetime(sa["date"])
    lab = lab.assign(date_ts=pd.to_datetime(lab["date"])).sort_values("date_ts")
    sa = sa.sort_values("date_ts")
    back = pd.merge_asof(sa, lab[["perma_ticker", "date_ts", "ticker", "cik"]], on="date_ts", by="perma_ticker", direction="backward")
    fwd = pd.merge_asof(sa, lab[["perma_ticker", "date_ts", "ticker", "cik"]], on="date_ts", by="perma_ticker", direction="forward")
    back["ticker"] = back["ticker"].fillna(fwd["ticker"])
    back["cik"] = back["cik"].fillna(fwd["cik"])
    sa = back.loc[back["ticker"].notna()]
    div = sa.loc[pd.to_numeric(sa["divCash"], errors="coerce").fillna(0) > 0]
    spl = sa.loc[(pd.to_numeric(sa["splitFactor"], errors="coerce").fillna(1) - 1).abs() > 1e-9]
    for frame, kind in ((div, "cash_div"), (spl, "split")):
        if frame.empty:
            continue
        action_frames.append(
            pd.DataFrame(
                {
                    "action_id": [str(uuid.uuid4()) for _ in range(len(frame))],
                    "cik": frame["cik"].astype("string"),
                    "ticker": frame["ticker"],
                    "ex_date": frame["date"],
                    "action_type": kind,
                    "ratio": pd.to_numeric(frame["splitFactor"], errors="coerce") if kind == "split" else None,
                    "amount": pd.to_numeric(frame["divCash"], errors="coerce") if kind == "cash_div" else None,
                    "spinoff_child_cik": None,
                    "source": "tiingo",
                    "ingestion_date": today,
                    "perma_ticker": frame["perma_ticker"],
                }
            )
        )
    actions = pd.concat(action_frames, ignore_index=True)
    actions = _apply_action_corrections(actions, prices, today)
    actions["ratio"] = actions["ratio"].astype("float64")
    actions["amount"] = actions["amount"].astype("float64")
    actions["spinoff_child_cik"] = actions["spinoff_child_cik"].astype("string")

    pt = pa.Table.from_pandas(prices, schema=schemas.PRICES, preserve_index=False)
    at = pa.Table.from_pandas(actions, schema=schemas.CORPORATE_ACTIONS, preserve_index=False)
    return pt, at


def rebuild(sm: pd.DataFrame, tenures: pd.DataFrame, *, end: dt.date | None = None) -> tuple[int, int]:
    """Rewrite prices.parquet and corporate_actions.parquet from vendor series.

    Curated spinoff rows (``source='manual_curated'``) are carried over from
    the existing action log; everything else is replaced.
    """
    from ..corporate_actions import spinoffs as spinoff_mod

    prices, actions = build_tables(sm, tenures, end=end)
    spins = spinoff_mod.build_spinoffs_table()
    if spins.num_rows:
        spins = spins.append_column(
            "perma_ticker", pa.nulls(spins.num_rows, type=pa.string())
        ) if "perma_ticker" not in spins.column_names else spins
        spins = spins.cast(schemas.CORPORATE_ACTIONS)
        actions = pa.concat_tables([actions, spins])
    writer.write_parquet(prices, writer.prices_path(), extra_metadata={"pitds.build": "entity_loader"})
    writer.write_parquet(actions, writer.corporate_actions_path(), extra_metadata={"pitds.build": "entity_loader"})
    return prices.num_rows, actions.num_rows
