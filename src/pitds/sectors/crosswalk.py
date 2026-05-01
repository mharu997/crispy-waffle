"""SIC → GICS Sector crosswalk.

This is the rigorous PIT-correct path to sector classification: every
EDGAR filing carries a SIC code, and we map it to the most-likely GICS
Sector via a 2-digit-prefix table. The mapping is approximate — SIC is a
1937 system and GICS is a 1999 system, so the joins aren't 1:1 — but it
gives a defensible "sector" field that's consistent with EDGAR's PIT
metadata.

Sources:
    - https://en.wikipedia.org/wiki/Global_Industry_Classification_Standard
    - https://www.sec.gov/search-filings/standard-industrial-classification-sic-code-list
    - Various open SIC↔GICS mappings published by academic researchers.

The 2016 GICS reform split Real Estate out of Financials; the 2018 reform
renamed Telecommunication Services to Communication Services and pulled
in some media companies. We hard-code the post-2018 names below; the
``sector_history`` materializer applies pre-2016 / pre-2018 names as
appropriate based on ``valid_from``.
"""

from __future__ import annotations

# 2-digit SIC prefix → GICS Sector (post-2018 names).
# This is a researcher-grade mapping; not authoritative. Use SIC directly
# when you need rigor; use this when you want a single sector field.
SIC_PREFIX_TO_GICS: dict[int, str] = {
    # Agriculture, Forestry, Fishing
    1: "Consumer Staples", 2: "Consumer Staples", 7: "Consumer Staples",
    8: "Materials", 9: "Materials",
    # Mining
    10: "Materials", 12: "Materials", 13: "Energy", 14: "Materials",
    # Construction
    15: "Industrials", 16: "Industrials", 17: "Industrials",
    # Manufacturing
    20: "Consumer Staples", 21: "Consumer Staples", 22: "Consumer Discretionary",
    23: "Consumer Discretionary", 24: "Materials", 25: "Consumer Discretionary",
    26: "Materials", 27: "Communication Services", 28: "Materials",
    29: "Energy", 30: "Materials", 31: "Consumer Discretionary",
    32: "Materials", 33: "Materials", 34: "Industrials",
    35: "Industrials", 36: "Information Technology", 37: "Industrials",
    38: "Health Care", 39: "Consumer Discretionary",
    # Transportation, Communications, Utilities
    40: "Industrials", 41: "Industrials", 42: "Industrials", 43: "Industrials",
    44: "Industrials", 45: "Industrials", 46: "Energy", 47: "Industrials",
    48: "Communication Services", 49: "Utilities",
    # Wholesale and Retail
    50: "Consumer Discretionary", 51: "Consumer Discretionary",
    52: "Consumer Discretionary", 53: "Consumer Discretionary",
    54: "Consumer Staples", 55: "Consumer Discretionary",
    56: "Consumer Discretionary", 57: "Consumer Discretionary",
    58: "Consumer Discretionary", 59: "Consumer Discretionary",
    # Finance, Insurance, Real Estate
    60: "Financials", 61: "Financials", 62: "Financials",
    63: "Financials", 64: "Financials", 65: "Real Estate",
    67: "Financials",
    # Services
    70: "Consumer Discretionary", 72: "Consumer Discretionary",
    73: "Information Technology", 75: "Consumer Discretionary",
    76: "Consumer Discretionary", 78: "Communication Services",
    79: "Communication Services", 80: "Health Care",
    81: "Industrials", 82: "Consumer Discretionary",
    83: "Industrials", 84: "Communication Services",
    86: "Industrials", 87: "Industrials", 88: "Consumer Discretionary",
    89: "Industrials",
    # Public Administration
    91: "Industrials", 92: "Industrials", 93: "Industrials",
    94: "Industrials", 95: "Industrials", 96: "Industrials",
    97: "Industrials", 99: "Industrials",
}

# Effective dates of GICS structural changes:
GICS_REAL_ESTATE_EFFECTIVE = "2016-09-19"  # Real Estate split out of Financials
GICS_COMM_SERVICES_EFFECTIVE = "2018-09-24"  # Telecom → Communication Services


def gics_sector_for_sic(sic: int | None) -> str | None:
    """Return the GICS Sector (post-2018 naming) for a SIC code."""
    if sic is None:
        return None
    prefix = sic // 100
    return SIC_PREFIX_TO_GICS.get(prefix)


def gics_sector_for_sic_at(sic: int | None, on_date: str) -> str | None:
    """Return the GICS Sector that would have applied on *on_date*.

    Reverses the post-2016 Real Estate split and the post-2018 Communication
    Services rename based on the date. ``on_date`` is an ISO yyyy-mm-dd string.
    """
    sector = gics_sector_for_sic(sic)
    if sector is None:
        return None
    if on_date < GICS_REAL_ESTATE_EFFECTIVE and sector == "Real Estate":
        return "Financials"
    if on_date < GICS_COMM_SERVICES_EFFECTIVE and sector == "Communication Services":
        return "Telecommunication Services"
    return sector
