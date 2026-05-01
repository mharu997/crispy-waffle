"""Concept aliasing across XBRL US-GAAP taxonomy versions.

US-GAAP changes its concept names over time. A field like "Revenue" was
``us-gaap:Revenues`` pre-2018 (before ASC 606), ``us-gaap:SalesRevenueNet``
in older taxonomies, and ``us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax``
post-2018. To answer "what was company X's revenue for FY 2014?" we have
to pick the right concept *for that point in time*.

This module defines canonical ``Field`` names (e.g. "revenue", "net_income",
"total_assets") and the ordered list of XBRL concepts that may carry them.
The PIT layer tries concepts in priority order until one yields a value.
"""

from __future__ import annotations

# Canonical name → priority-ordered list of XBRL concepts.
# Priority order is "most-specific most-modern first." Earlier concepts
# in the list win when both are present for the same (cik, period).
FIELD_TO_CONCEPTS: dict[str, list[str]] = {
    "revenue": [
        "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
        "us-gaap:Revenues",
        "us-gaap:SalesRevenueNet",
        "us-gaap:SalesRevenueGoodsNet",
    ],
    "cost_of_revenue": [
        "us-gaap:CostOfRevenue",
        "us-gaap:CostOfGoodsAndServicesSold",
        "us-gaap:CostOfGoodsSold",
        "us-gaap:CostOfServicesSold",
    ],
    "gross_profit": [
        "us-gaap:GrossProfit",
    ],
    "operating_income": [
        "us-gaap:OperatingIncomeLoss",
    ],
    "net_income": [
        "us-gaap:NetIncomeLoss",
        "us-gaap:ProfitLoss",
    ],
    "eps_basic": [
        "us-gaap:EarningsPerShareBasic",
    ],
    "eps_diluted": [
        "us-gaap:EarningsPerShareDiluted",
    ],
    "total_assets": [
        "us-gaap:Assets",
    ],
    "total_liabilities": [
        "us-gaap:Liabilities",
    ],
    "total_equity": [
        "us-gaap:StockholdersEquity",
        "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ],
    "cash": [
        "us-gaap:CashAndCashEquivalentsAtCarryingValue",
        "us-gaap:Cash",
    ],
    "long_term_debt": [
        "us-gaap:LongTermDebtNoncurrent",
        "us-gaap:LongTermDebt",
    ],
    "shares_outstanding": [
        "us-gaap:CommonStockSharesOutstanding",
        "dei:EntityCommonStockSharesOutstanding",
    ],
    "operating_cash_flow": [
        "us-gaap:NetCashProvidedByUsedInOperatingActivities",
        "us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    ],
    "capex": [
        "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
    ],
}


def concepts_for(field: str) -> list[str]:
    """Return the priority-ordered list of XBRL concepts for *field*."""
    if field not in FIELD_TO_CONCEPTS:
        raise KeyError(
            f"Unknown field {field!r}. Known: {sorted(FIELD_TO_CONCEPTS)}"
        )
    return list(FIELD_TO_CONCEPTS[field])


def all_canonical_fields() -> list[str]:
    return sorted(FIELD_TO_CONCEPTS)
