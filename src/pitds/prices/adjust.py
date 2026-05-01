"""On-read split/dividend adjustment from the action log.

PITDS stores **unadjusted** OHLCV. Adjusted prices are computed on read,
so the dataset is a deterministic function of (raw prices + action log).
This isolates us from yfinance/Tiingo adjustment-logic changes.

The adjustment math:

    For a series of corporate actions e_1 < e_2 < ... < e_n with split
    ratios r_i and cash dividends d_i (per share), the multiplier
    applied to a price on date t to view it "as if all subsequent
    actions had not happened" is:

        m(t) = ∏_{e_i > t} (1/r_i)             (split-only adjustment)
        m(t) = ∏_{e_i > t} (1 - d_i/close_{e_i-1}) * (1/r_i)
                                                (split + dividend adjustment)

Volume is multiplied by ``r_i`` (more shares post-split means proportionally
more volume in pre-split currency).

We compute this in DuckDB because the cumulative product is a window
function and DuckDB executes it at file-scan speed.
"""

from __future__ import annotations

import logging
from typing import Literal

import duckdb

log = logging.getLogger(__name__)

AdjustMode = Literal["none", "split", "split_div"]


def adjusted_prices_query(
    *,
    prices_path: str,
    actions_path: str | None = None,
    mode: AdjustMode = "split_div",
) -> str:
    """Return a DuckDB SQL query that yields adjusted OHLCV on read.

    The returned query takes parameters bound at runtime; embed it as a
    subquery (e.g. ``CREATE VIEW v_prices_adj AS <this query>``).
    """
    if mode == "none":
        return f"SELECT * FROM read_parquet({prices_path!r})"

    actions_clause = (
        f"FROM read_parquet({actions_path!r}) WHERE action_type IN ('split', 'cash_div')"
        if actions_path
        else "FROM (SELECT NULL AS ticker, NULL AS ex_date, NULL AS action_type, NULL AS ratio, NULL AS amount LIMIT 0)"
    )

    # Reference a.prev_close (joined from actions_with_prev_close), NOT
    # p_prev.close (which only exists inside the CTE definition above).
    div_term = (
        "(1.0 - COALESCE(a.amount, 0) / NULLIF(a.prev_close, 0))"
        if mode == "split_div"
        else "1.0"
    )

    return f"""
WITH actions AS (
    SELECT ticker, ex_date, action_type, ratio, amount
    {actions_clause}
),
prices AS (
    SELECT * FROM read_parquet({prices_path!r})
),
-- For dividend adjustments we need the close price on the day BEFORE the
-- ex-date; pre-join here so the cumulative product stays linear.
actions_with_prev_close AS (
    SELECT
        a.ticker,
        a.ex_date,
        a.action_type,
        a.ratio,
        a.amount,
        p_prev.close AS prev_close
    FROM actions a
    LEFT JOIN prices p_prev
        ON  a.ticker = p_prev.ticker
        AND p_prev.date = (
            SELECT MAX(p2.date) FROM prices p2
            WHERE p2.ticker = a.ticker AND p2.date < a.ex_date
        )
),
-- Per-row adjustment factor. Strategy: pre-aggregate cumulative
-- log-adjustments per (ticker, date) over the actions log, then look
-- them up per price row with a single LEFT JOIN. This avoids the
-- "EXP cannot wrap a window function" syntax constraint.
log_adj_per_action AS (
    SELECT
        ticker,
        ex_date,
        CASE
            WHEN action_type = 'split' THEN -LN(NULLIF(ratio, 0))
            WHEN action_type = 'cash_div' THEN LN({div_term})
            ELSE 0
        END AS log_adj
    FROM actions_with_prev_close a
),
-- For each row in prices, sum log-adjustments for actions strictly
-- AFTER that row's date.
log_adj_per_price AS (
    SELECT
        p.ticker,
        p.date,
        COALESCE(SUM(la.log_adj), 0) AS log_factor
    FROM prices p
    LEFT JOIN log_adj_per_action la
        ON la.ticker = p.ticker
       AND la.ex_date > p.date
    GROUP BY p.ticker, p.date
),
adjusted AS (
    SELECT
        p.*,
        EXP(lap.log_factor) AS adj_factor
    FROM prices p
    JOIN log_adj_per_price lap
      ON lap.ticker = p.ticker AND lap.date = p.date
)
SELECT
    date, ticker, cik, figi,
    CAST(open  AS DOUBLE) * adj_factor AS open_adj,
    CAST(high  AS DOUBLE) * adj_factor AS high_adj,
    CAST(low   AS DOUBLE) * adj_factor AS low_adj,
    CAST(close AS DOUBLE) * adj_factor AS close_adj,
    -- Volume scales inversely to price (more shares post-split).
    volume / NULLIF(adj_factor, 0) AS volume_adj,
    open, high, low, close, volume,
    source, ingestion_date,
    adj_factor
FROM adjusted
ORDER BY ticker, date
"""


def adjusted_prices(
    *,
    prices_path: str,
    actions_path: str | None = None,
    mode: AdjustMode = "split_div",
):
    """Run the adjustment query and return a DuckDB relation."""
    sql = adjusted_prices_query(
        prices_path=prices_path, actions_path=actions_path, mode=mode
    )
    return duckdb.sql(sql)
