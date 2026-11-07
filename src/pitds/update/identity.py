"""One code path for (re)building everything keyed on the identity spine.

Used by both ``pitds build-security-master`` and the nightly job, so the
daily refresh produces exactly what a full rebuild would:

1. live-edge membership (Wikipedia changes newer than fja05680)
2. security master (vendor entity, PIT CIK, as-of ticker)
3. prices + corporate actions from the same vendor series
4. ticker_history, constituents_daily / constituent_changes
5. sector_history (PIT, from dated Wikipedia snapshots)
6. EDGAR facts for any CIK that has none yet (new members)

Vendor series are cached incrementally (see ``prices.tiingo``), so a nightly
run re-pulls ~2 weeks per active entity, not full histories.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class IdentityBuild:
    live_edge_rows: int = 0
    security_master_rows: int = 0
    price_status: dict = field(default_factory=dict)
    price_rows: int = 0
    action_rows: int = 0
    ticker_history_rows: int = 0
    constituents_rows: int = 0
    change_events: int = 0
    sector_rows: int = 0
    new_fundamentals_ciks: int = 0


def rebuild_identity(
    *,
    live_edge: bool = True,
    prices: bool = True,
    fundamentals_for_new: bool = False,
    refresh_wiki: bool = False,
    refresh_meta: bool = False,
) -> IdentityBuild:
    from ..config import settings
    from ..constituents import live_edge as le
    from ..constituents.changes import write_constituents_outputs
    from ..constituents.corrections import apply_corrections
    from ..constituents.fja05680 import _parse_tenures, fetch_snapshot
    from ..constituents.wikipedia_history import build_snapshots
    from ..identifiers import tiingo_meta
    from ..identifiers.security_master import (
        build_security_master,
        load_security_master,
        relevant_tenures,
    )
    from ..identifiers.ticker_history import write_from_security_master
    from ..sectors.reconstruct import write_sector_history
    from ..store import writer

    out = IdentityBuild()
    if refresh_meta:
        tiingo_meta.fetch_meta(refresh=True)
        tiingo_meta.load_meta.cache_clear()
    if refresh_wiki:
        build_snapshots()

    snap = fetch_snapshot()
    if live_edge:
        raw = _parse_tenures(
            (settings.raw_dir / "fja05680" / snap.fetched_on.isoformat() / "sp500_ticker_start_end.csv").read_bytes()
        )
        prev = load_security_master() if writer.security_master_path().exists() else None
        out.live_edge_rows = len(le.refresh(apply_corrections(raw, live_edge=False), prev))
        snap = fetch_snapshot()  # reload so the new live edge is applied

    sm = build_security_master(snap.ticker_tenures)
    out.security_master_rows = len(sm)
    out.price_status = sm["price_status"].value_counts().to_dict()

    if prices:
        from ..prices.entity_loader import rebuild

        out.price_rows, out.action_rows = rebuild(sm, relevant_tenures(snap.ticker_tenures))

    out.ticker_history_rows = write_from_security_master(sm)
    out.constituents_rows, out.change_events = write_constituents_outputs(snap.ticker_tenures, snap.current_snapshot)
    out.sector_rows = write_sector_history()

    if fundamentals_for_new:
        out.new_fundamentals_ciks = backfill_missing_fundamentals(sm)
    return out


def backfill_missing_fundamentals(sm) -> int:
    """Fetch EDGAR companyfacts for security-master CIKs with no facts on disk."""
    import duckdb
    import pyarrow as pa
    import pyarrow.compute as pc

    from ..fundamentals.companyfacts_api import fetch_companyfacts, normalize_facts
    from ..store import writer as w

    glob = str(w.fundamentals_facts_dir() / "*" / "*.parquet")
    have = set(duckdb.sql(f"SELECT DISTINCT cik FROM read_parquet('{glob}', hive_partitioning=true)").df()["cik"])
    todo = sorted(set(sm["cik"].dropna()) - have)
    tables = []
    for cik in todo:
        try:
            raw = fetch_companyfacts(cik)
            if raw is None:
                continue
            t = normalize_facts(cik, raw)
            if t.num_rows:
                tables.append(t)
        except Exception as e:  # one bad CIK must not stop the run
            log.warning("companyfacts failed for %s: %s", cik, e)
    if not tables:
        return 0
    combined = pa.concat_tables(tables)
    combined = combined.append_column("filed_year", pc.year(pc.cast(combined["filed"], pa.timestamp("us"))))
    w.append_to_partition(
        combined,
        w.fundamentals_facts_dir(),
        partition_col="filed_year",
        primary_key=["cik", "concept", "period_start", "period_end", "accession_number"],
    )
    return len(tables)
