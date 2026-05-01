"""FRED loader for SP500 index validation (Tier D).

FRED's ``SP500`` series gives the daily S&P 500 index level (price-return,
not total-return) back to 1957. We use it to validate our reconstructed
cap-weight index — a properly built PITDS should track FRED to within a
few bps over any window, since both reflect the same underlying index.
"""

from __future__ import annotations

import datetime as dt
import logging

import pandas as pd

log = logging.getLogger(__name__)

FRED_CSV_URL = (
    "https://fred.stlouisfed.org/graph/fredgraph.csv"
    "?id=SP500&cosd={start}&coed={end}"
)


def fetch_sp500_index(start: dt.date, end: dt.date) -> pd.DataFrame:
    """Return FRED's daily SP500 index level series in [start, end]."""
    url = FRED_CSV_URL.format(start=start.isoformat(), end=end.isoformat())
    df = pd.read_csv(url)
    # FRED column is named 'SP500' (capital, no spaces).
    value_col = "SP500" if "SP500" in df.columns else df.columns[1]
    df = df.rename(columns={"observation_date": "date", "DATE": "date", value_col: "sp500"})
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["sp500"] = pd.to_numeric(df["sp500"], errors="coerce")
    return df[["date", "sp500"]].dropna().reset_index(drop=True)
