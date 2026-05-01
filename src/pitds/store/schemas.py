"""Arrow schemas — the single source of truth for all PITDS table structure.

Every Parquet file written under ``data/processed/`` conforms to one of
these schemas. The PIT contract is encoded in the column types and primary
key declarations: tickers can be reassigned over time, so ``cik`` is the
canonical join key in the analytical tables (``prices``,
``fundamentals_facts``, ``sector_history``). Restatements are append-only:
every fact is keyed by its SEC ``accession_number`` and never overwritten.
"""

from __future__ import annotations

import pyarrow as pa

# ---------------------------------------------------------------------------
# Reusable type aliases
# ---------------------------------------------------------------------------

# CIK is a zero-padded 10-character string per SEC convention (e.g. "0000320193").
# Storing as string preserves the leading zeros that the SEC's APIs require.
CIK = pa.string()

# FIGI is OpenFIGI's 12-character durable identifier. Use as fallback canonical
# entity key when CIK changes (rare, but happens at reorganizations).
FIGI = pa.string()

# Tickers are point-in-time. Always pair with a date or a (valid_from, valid_to) span.
TICKER = pa.string()

# Dates are calendar days. PITDS does not store time-of-day; filings after market
# close are normalized to the next business day before being stored.
DATE = pa.date32()

# Prices and dividend amounts use float64. Decimal128 would be marginally
# more precise but creates conversion friction with pandas/yfinance/Tiingo
# which all surface float64. 15-17 significant digits is far more than
# sufficient for retail US equity prices (post-decimalization tick size
# is $0.01, well within float64 representational precision).
PRICE = pa.float64()
DIVIDEND_AMOUNT = pa.float64()

# UUID-as-string for synthetic primary keys.
UUID = pa.string()


# ---------------------------------------------------------------------------
# Constituent changes — the event log
# ---------------------------------------------------------------------------

CONSTITUENT_CHANGES = pa.schema(
    [
        pa.field("event_id", UUID, nullable=False),
        pa.field("cik", CIK, nullable=True),  # null only if CIK lookup failed
        pa.field("ticker", TICKER, nullable=False),
        pa.field("figi", FIGI, nullable=True),
        # event_type ∈ {"add", "remove", "rename"}
        pa.field("event_type", pa.string(), nullable=False),
        # Both dates are required for adds/removes. announcement_date may equal
        # effective_date for emergency removals (e.g. Lehman 2008-09-15).
        pa.field("announcement_date", DATE, nullable=True),
        pa.field("effective_date", DATE, nullable=False),
        # Renames carry both old and new tickers; for add/remove these are null.
        pa.field("old_ticker", TICKER, nullable=True),
        pa.field("new_ticker", TICKER, nullable=True),
        # Free-form: "m&a", "bankruptcy", "rebalance", "spinoff", "rename", etc.
        pa.field("reason", pa.string(), nullable=True),
        # Provenance: which loader inserted this row.
        pa.field("source", pa.string(), nullable=False),
        pa.field("ingestion_date", DATE, nullable=False),
        pa.field("notes", pa.string(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Constituents daily — materialized membership matrix
# ---------------------------------------------------------------------------

# Derived from CONSTITUENT_CHANGES. One row per (date, member). Used for fast
# "who was in the index on date T?" queries without recomputing the event log
# fold each time.

CONSTITUENTS_DAILY = pa.schema(
    [
        pa.field("date", DATE, nullable=False),
        pa.field("ticker", TICKER, nullable=False),
        # cik is populated by the identifiers backfill; nullable until then,
        # and may legitimately stay null for very old delisted tickers with
        # no EDGAR presence.
        pa.field("cik", CIK, nullable=True),
        pa.field("figi", FIGI, nullable=True),
        # True if the member was already announced but not yet effective on this
        # date. Useful for strategies that trade the announcement effect.
        pa.field("announced_only", pa.bool_(), nullable=False),
    ]
)


# ---------------------------------------------------------------------------
# Prices — single Parquet, ~3M rows post-2010
# ---------------------------------------------------------------------------

# Unadjusted OHLCV. Adjusted prices are computed on read via prices/adjust.py
# from corporate_actions, never stored. This makes the dataset a deterministic
# function of (raw prices, action log).

PRICES = pa.schema(
    [
        pa.field("date", DATE, nullable=False),
        pa.field("ticker", TICKER, nullable=False),  # ticker as of this date
        # cik is the canonical join key for active filers, but ~40% of
        # historical/delisted tickers have no resolved CIK in our free-data
        # pipeline (Phase B OpenFIGI integration improves this). Nullable
        # so those rows can still be stored — the (ticker, date) pair
        # uniquely identifies a row in the absence of CIK.
        pa.field("cik", CIK, nullable=True),
        pa.field("figi", FIGI, nullable=True),
        pa.field("open", PRICE, nullable=True),
        pa.field("high", PRICE, nullable=True),
        pa.field("low", PRICE, nullable=True),
        pa.field("close", PRICE, nullable=False),  # close is required
        pa.field("volume", pa.int64(), nullable=True),
        # Provenance: 'tiingo' | 'yfinance' | 'stooq'.
        pa.field("source", pa.string(), nullable=False),
        pa.field("ingestion_date", DATE, nullable=False),
    ]
)


# ---------------------------------------------------------------------------
# Fundamentals facts — partitioned by year(filed)
# ---------------------------------------------------------------------------

# Append-only. Restatements add new rows with later filed dates and different
# accession_numbers. The PIT layer (fundamentals/pit_layer.py) selects vintage
# at query time via a window function over (cik, concept, period_*).

FUNDAMENTALS_FACTS = pa.schema(
    [
        pa.field("cik", CIK, nullable=False),
        # Fully-qualified XBRL concept, e.g. "us-gaap:Revenues".
        pa.field("concept", pa.string(), nullable=False),
        # XBRL unit (e.g. "USD", "shares", "USD/shares"). Reject non-USD currencies
        # at ingestion unless an FX layer is built.
        pa.field("unit", pa.string(), nullable=False),
        # period_start is null for instantaneous facts (balance sheet items).
        pa.field("period_start", DATE, nullable=True),
        pa.field("period_end", DATE, nullable=False),
        pa.field("val", pa.float64(), nullable=False),
        # SEC accession number — primary key component, distinguishes vintages.
        pa.field("accession_number", pa.string(), nullable=False),
        # The "knowable on" date. Heart of the PIT contract: queries filter
        # WHERE filed <= as_of.
        pa.field("filed", DATE, nullable=False),
        # Form type: "10-K", "10-Q", "10-K/A", "8-K", etc.
        pa.field("form", pa.string(), nullable=False),
        pa.field("fy", pa.int32(), nullable=True),
        # Fiscal period: "Q1" | "Q2" | "Q3" | "FY".
        pa.field("fp", pa.string(), nullable=True),
        # XBRL frame designator, e.g. "CY2018Q3I". Useful for cross-company ratios.
        pa.field("frame", pa.string(), nullable=True),
        pa.field("ingestion_date", DATE, nullable=False),
    ]
)


# ---------------------------------------------------------------------------
# Sector history — bitemporal classification
# ---------------------------------------------------------------------------

# Bitemporal: (valid_from, valid_to) defines the period the classification
# applied; ingestion_date stamps when we recorded it.
#
# sic_code is PIT-rigorous (extracted from EDGAR filing headers).
# gics_sector_observed is approximate (Wikipedia revision history).
# gics_sector_recon is derived from sic_code via crosswalk.

SECTOR_HISTORY = pa.schema(
    [
        pa.field("cik", CIK, nullable=False),
        pa.field("ticker", TICKER, nullable=True),
        pa.field("valid_from", DATE, nullable=False),
        pa.field("valid_to", DATE, nullable=True),  # null = currently valid
        pa.field("sic_code", pa.int32(), nullable=True),
        pa.field("gics_sector_observed", pa.string(), nullable=True),
        pa.field("gics_sector_recon", pa.string(), nullable=True),
        pa.field("source", pa.string(), nullable=False),
        pa.field("ingestion_date", DATE, nullable=False),
    ]
)


# ---------------------------------------------------------------------------
# Corporate actions — the action log used by adjust.py
# ---------------------------------------------------------------------------

# Splits, dividends, and spinoffs. Stock splits use ratio (e.g. 2.0 for a 2:1
# split, 0.1 for a 1:10 reverse split). Cash dividends use amount (per share).

CORPORATE_ACTIONS = pa.schema(
    [
        pa.field("action_id", UUID, nullable=False),
        pa.field("cik", CIK, nullable=False),
        pa.field("ticker", TICKER, nullable=False),  # ticker on ex_date
        pa.field("ex_date", DATE, nullable=False),
        # action_type ∈ {"split", "cash_div", "stock_div", "spinoff"}.
        pa.field("action_type", pa.string(), nullable=False),
        pa.field("ratio", pa.float64(), nullable=True),  # for splits
        pa.field("amount", DIVIDEND_AMOUNT, nullable=True),  # for cash dividends
        pa.field("spinoff_child_cik", CIK, nullable=True),  # for spinoffs
        pa.field("source", pa.string(), nullable=False),
        pa.field("ingestion_date", DATE, nullable=False),
    ]
)


# ---------------------------------------------------------------------------
# Ticker history — handle ticker reassignment
# ---------------------------------------------------------------------------

# A ticker symbol may be reassigned to a different company over time
# (most famously: the symbol "FB" became "META" in 2022; reused symbols
# are rarer but exist). Lookup table mapping (ticker, date) → CIK.

TICKER_HISTORY = pa.schema(
    [
        pa.field("ticker", TICKER, nullable=False),
        pa.field("cik", CIK, nullable=False),
        pa.field("valid_from", DATE, nullable=False),
        pa.field("valid_to", DATE, nullable=True),  # null = currently valid
        pa.field("source", pa.string(), nullable=False),
        pa.field("ingestion_date", DATE, nullable=False),
    ]
)


# ---------------------------------------------------------------------------
# Schema registry
# ---------------------------------------------------------------------------

SCHEMAS: dict[str, pa.Schema] = {
    "constituent_changes": CONSTITUENT_CHANGES,
    "constituents_daily": CONSTITUENTS_DAILY,
    "prices": PRICES,
    "fundamentals_facts": FUNDAMENTALS_FACTS,
    "sector_history": SECTOR_HISTORY,
    "corporate_actions": CORPORATE_ACTIONS,
    "ticker_history": TICKER_HISTORY,
}


def get_schema(name: str) -> pa.Schema:
    """Return the schema registered under *name*."""
    try:
        return SCHEMAS[name]
    except KeyError as e:
        raise KeyError(
            f"Unknown schema {name!r}. Known: {sorted(SCHEMAS)}"
        ) from e
