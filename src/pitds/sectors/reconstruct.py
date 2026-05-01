"""Materialize ``sector_history.parquet`` from EDGAR SIC + crosswalk.

For each CIK in our ticker_history, we pull the SIC from EDGAR submissions
and emit a row in ``sector_history`` with the SIC code and the derived
GICS Sector. ``valid_from`` is set to the CIK's earliest known constituent
date (proxied by ticker_history.valid_from); ``valid_to`` is null
(currently valid).

Phase C will refine this by parsing the SIC from each individual filing
header, giving multi-row sector_history when SIC actually changes.
"""

from __future__ import annotations

import datetime as dt
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..store import schemas, writer
from . import crosswalk, gics_wiki, sic_edgar

log = logging.getLogger(__name__)


def build_sector_history(
    *,
    ciks_with_meta: pd.DataFrame,
    refresh: bool = False,
) -> pa.Table:
    """Build ``sector_history`` rows for each CIK in *ciks_with_meta*.

    *ciks_with_meta* must have columns ``cik``, ``ticker``, ``valid_from``.
    Optional ``valid_to``.
    """
    today = dt.date.today()
    rows: list[dict] = []

    # Pull current Wikipedia GICS Sector mapping for the active universe.
    # Phase B uses the *current* mapping as a snapshot; Phase C will walk
    # revision history for true PIT sectors.
    try:
        wiki = gics_wiki.extract_current_gics_table()
        ticker_to_gics = dict(
            zip(
                wiki["symbol"].astype(str).str.upper(),
                wiki["gics_sector"].astype(str),
                strict=False,
            )
        )
    except Exception as e:
        log.warning("Wikipedia GICS table fetch failed: %s", e)
        ticker_to_gics = {}

    seen_ciks: dict[str, tuple[int | None, str | None]] = {}
    for r in ciks_with_meta.itertuples(index=False):
        cik = str(r.cik).zfill(10)
        if cik not in seen_ciks:
            try:
                sic, _desc = sic_edgar.get_sic_for_cik(cik)
            except Exception as e:
                log.warning("SIC lookup failed for %s: %s", cik, e)
                sic = None
            seen_ciks[cik] = (sic, _desc if sic else None)
        sic, _ = seen_ciks[cik]

        valid_from = r.valid_from if isinstance(r.valid_from, dt.date) else dt.date.fromisoformat(str(r.valid_from)[:10])
        valid_to = (
            r.valid_to
            if hasattr(r, "valid_to") and isinstance(r.valid_to, dt.date)
            else None
        )

        gics_recon = crosswalk.gics_sector_for_sic_at(sic, valid_from.isoformat())
        gics_observed = ticker_to_gics.get(str(r.ticker).upper())

        rows.append(
            {
                "cik": cik,
                "ticker": r.ticker,
                "valid_from": valid_from,
                "valid_to": valid_to,
                "sic_code": sic,
                "gics_sector_observed": gics_observed,
                "gics_sector_recon": gics_recon,
                "source": "edgar_submissions+wikipedia",
                "ingestion_date": today,
            }
        )

    df = pd.DataFrame(rows)
    df["sic_code"] = pd.to_numeric(df["sic_code"], errors="coerce").astype("Int32")
    return pa.Table.from_pandas(
        df, schema=schemas.SECTOR_HISTORY, preserve_index=False
    )


def write_sector_history(*, refresh: bool = False) -> int:
    """Materialize sector_history.parquet from current ticker_history."""
    th_path = writer.ticker_history_path()
    if not th_path.exists():
        log.error("ticker_history.parquet not built; run `pitds backfill-identifiers`")
        return 0

    df = pq.read_table(th_path).to_pandas()
    table = build_sector_history(ciks_with_meta=df, refresh=refresh)
    writer.write_parquet(table, writer.sector_history_path())
    return table.num_rows
