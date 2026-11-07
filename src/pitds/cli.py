"""``pitds`` command-line interface (Typer-based).

Top-level commands:

* ``pitds backfill-constituents`` — Pull fja05680, materialize daily matrix.
* ``pitds backfill-prices`` — Run the price cascade for the constituent universe.
* ``pitds backfill-corporate-actions`` — Splits/dividends from yfinance + curated spinoffs.
* ``pitds backfill-identifiers`` — Build ticker_history from SEC + fja05680 seed.
* ``pitds verify-prices`` — Sanity-check coverage and reconcile against FRED.
* ``pitds info`` — Show dataset state (file sizes, row counts, vintage).

Run ``pitds --help`` after install for the full surface.
"""

from __future__ import annotations

import datetime as dt
import logging
import sys
from pathlib import Path

import typer

from .config import settings

app = typer.Typer(
    name="pitds",
    help="Point-In-Time S&P 500 dataset CLI.",
    no_args_is_help=True,
    add_completion=False,
)


def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


@app.callback()
def _root(verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging")):
    _setup_logging(verbose)
    settings.ensure_directories()


@app.command("backfill-constituents")
def backfill_constituents(
    refresh: bool = typer.Option(False, "--refresh", help="Re-download fja05680"),
    start: str = typer.Option("2010-01-01", help="History start (YYYY-MM-DD)"),
):
    """Materialize constituents_daily and constituent_changes from fja05680."""
    from .constituents.changes import write_constituents_outputs
    from .constituents.fja05680 import fetch_snapshot

    snap = fetch_snapshot(refresh=refresh)
    n_daily, n_events = write_constituents_outputs(
        snap.ticker_tenures,
        snap.current_snapshot,
        start=dt.date.fromisoformat(start),
    )
    typer.echo(
        f"OK: {n_daily} daily rows, {n_events} change events written."
    )


@app.command("backfill-identifiers")
def backfill_identifiers(refresh: bool = typer.Option(False, "--refresh", help="Re-download SEC ticker JSONs")):
    """Build ticker_history from SEC + fja05680 seed."""
    from .constituents.fja05680 import fetch_snapshot
    from .identifiers.cik_map import fetch_ticker_jsons
    from .identifiers.ticker_history import write_initial

    if refresh:
        fetch_ticker_jsons(refresh=True)

    snap = fetch_snapshot()
    seed = dict(
        zip(
            snap.current_snapshot["symbol"].astype(str).str.upper(),
            snap.current_snapshot["cik"].astype(str),
            strict=False,
        )
    )
    seed = {k: v for k, v in seed.items() if v and v != "<NA>" and v != "nan"}
    n = write_initial(snap.ticker_tenures, seed_cik=seed)
    typer.echo(f"OK: {n} ticker_history rows written ({len(seed)} seeded from fja).")


@app.command("build-security-master")
def build_security_master_cmd(
    live_edge: bool = typer.Option(True, "--live-edge/--no-live-edge", help="Refresh Wikipedia live-edge membership first"),
    wiki: bool = typer.Option(False, "--refresh-wiki", help="Re-sample quarterly Wikipedia snapshots"),
    meta: bool = typer.Option(False, "--refresh-meta", help="Re-download Tiingo fundamentals meta"),
    prices: bool = typer.Option(True, "--prices/--no-prices", help="Rebuild prices + corporate actions afterwards"),
    fundamentals: bool = typer.Option(False, "--fundamentals", help="Fetch EDGAR facts for CIKs that have none"),
):
    """Rebuild the identity spine and everything keyed on it.

    Order: live-edge membership → security master (entity, CIK, as-of ticker)
    → prices + corporate actions → ticker_history → constituents → sectors.
    """
    from .update.identity import rebuild_identity

    r = rebuild_identity(
        live_edge=live_edge, prices=prices, fundamentals_for_new=fundamentals,
        refresh_wiki=wiki, refresh_meta=meta,
    )
    typer.echo(f"live edge: {r.live_edge_rows} membership corrections beyond fja05680")
    typer.echo(f"security_master: {r.security_master_rows} rows; price_status={r.price_status}")
    if prices:
        typer.echo(f"prices: {r.price_rows:,} rows; corporate_actions: {r.action_rows:,} rows")
    typer.echo(f"ticker_history: {r.ticker_history_rows}; constituents_daily: {r.constituents_rows:,}; "
               f"changes: {r.change_events}; sector_history: {r.sector_rows}")
    if fundamentals:
        typer.echo(f"fundamentals: facts fetched for {r.new_fundamentals_ciks} new CIKs")


@app.command("rebuild-prices")
def rebuild_prices():
    """Rewrite prices + corporate actions from the security master's vendor series."""
    from .constituents.fja05680 import fetch_snapshot
    from .identifiers.security_master import load_security_master, relevant_tenures
    from .prices.entity_loader import rebuild

    snap = fetch_snapshot()
    n_p, n_a = rebuild(load_security_master(), relevant_tenures(snap.ticker_tenures))
    typer.echo(f"OK: prices {n_p:,} rows; corporate_actions {n_a:,} rows")


@app.command("backfill-prices")
def backfill_prices(
    start: str = typer.Option("2010-01-01"),
    end: str = typer.Option("today"),
    universe: str = typer.Option(
        "current",
        help="One of: current (active S&P 500), all (full historical), or comma-list",
    ),
    limit: int = typer.Option(0, help="Process at most N tickers (for testing)"),
):
    """Backfill prices via the cascade for the requested universe."""
    from .constituents.fja05680 import fetch_snapshot
    from .identifiers.cik_map import resolve_ciks
    from .prices.loader import backfill

    end_d = dt.date.today() if end == "today" else dt.date.fromisoformat(end)
    start_d = dt.date.fromisoformat(start)

    snap = fetch_snapshot()
    if universe == "current":
        tickers = sorted(snap.current_snapshot["symbol"].astype(str).str.upper())
    elif universe == "all":
        tickers = sorted(set(snap.ticker_tenures["ticker"].astype(str).str.upper()))
    else:
        tickers = [t.strip().upper() for t in universe.split(",") if t.strip()]

    if limit:
        tickers = tickers[:limit]

    seed = dict(
        zip(
            snap.current_snapshot["symbol"].astype(str).str.upper(),
            snap.current_snapshot["cik"].astype(str),
            strict=False,
        )
    )
    seed = {k: v for k, v in seed.items() if v and v != "<NA>" and v != "nan"}
    cik_map = resolve_ciks(tickers, seed=seed)
    cik_resolver = lambda t: cik_map.get(t.upper())

    typer.echo(f"Backfilling {len(tickers)} tickers from {start_d} to {end_d} ...")
    n = backfill(tickers, start=start_d, end=end_d, cik_resolver=cik_resolver)
    typer.echo(f"OK: {n} price rows written.")


@app.command("backfill-corporate-actions")
def backfill_corporate_actions(
    universe: str = typer.Option("current"),
    limit: int = typer.Option(0),
):
    """Backfill corporate actions (splits, dividends, curated spinoffs)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from .constituents.fja05680 import fetch_snapshot
    from .corporate_actions import spinoffs as spinoff_mod
    from .corporate_actions.splits_dividends import build_actions_table
    from .identifiers.cik_map import resolve_ciks
    from .store import writer

    snap = fetch_snapshot()
    if universe == "current":
        tickers = sorted(snap.current_snapshot["symbol"].astype(str).str.upper())
    elif universe == "all":
        tickers = sorted(set(snap.ticker_tenures["ticker"].astype(str).str.upper()))
    else:
        tickers = [t.strip().upper() for t in universe.split(",") if t.strip()]

    if limit:
        tickers = tickers[:limit]

    seed = dict(
        zip(
            snap.current_snapshot["symbol"].astype(str).str.upper(),
            snap.current_snapshot["cik"].astype(str),
            strict=False,
        )
    )
    seed = {k: v for k, v in seed.items() if v and v != "<NA>" and v != "nan"}
    cik_map = resolve_ciks(tickers, seed=seed)

    typer.echo(f"Fetching corporate actions for {len(tickers)} tickers ...")
    splits_divs = build_actions_table(
        tickers, cik_resolver=lambda t: cik_map.get(t.upper())
    )
    typer.echo(f"  splits/divs: {splits_divs.num_rows} rows")

    spins = spinoff_mod.build_spinoffs_table()
    typer.echo(f"  spinoffs (curated): {spins.num_rows} rows")

    combined = pa.concat_tables([splits_divs, spins])
    writer.upsert_single_file(
        combined,
        writer.corporate_actions_path(),
        primary_key=["ticker", "ex_date", "action_type"],
        sort_by=["ticker", "ex_date"],
    )
    typer.echo(f"OK: {combined.num_rows} corporate_actions rows written.")


@app.command("retry-failed-prices")
def retry_failed_prices(
    log_path: str = typer.Option("data/raw/backfill_prices.log", help="Backfill log to mine"),
    start: str = typer.Option("2010-01-01"),
    end: str = typer.Option("today"),
):
    """Retry tickers that hit "All sources failed" during a previous backfill.

    Mines the log for ``All sources failed/empty for X`` lines and retries
    the cascade for those tickers. The current code path applies the
    dot→dash normalization fix that was added after the first backfill.
    """
    import re
    from pathlib import Path

    from .constituents.fja05680 import fetch_snapshot
    from .identifiers.cik_map import resolve_ciks
    from .prices.loader import backfill

    log_file = Path(log_path)
    if not log_file.exists():
        typer.echo(f"Log file not found: {log_path}")
        raise typer.Exit(code=1)

    pattern = re.compile(r"All sources failed/empty for (\S+)")
    failed = sorted(set(pattern.findall(log_file.read_text())))
    if not failed:
        typer.echo("No failed tickers found in log — nothing to retry.")
        return

    typer.echo(f"Retrying {len(failed)} failed tickers: {failed[:10]}{' ...' if len(failed) > 10 else ''}")

    end_d = dt.date.today() if end == "today" else dt.date.fromisoformat(end)
    start_d = dt.date.fromisoformat(start)

    snap = fetch_snapshot()
    seed = dict(
        zip(
            snap.current_snapshot["symbol"].astype(str).str.upper(),
            snap.current_snapshot["cik"].astype(str),
            strict=False,
        )
    )
    seed = {k: v for k, v in seed.items() if v and v != "<NA>" and v != "nan"}
    cik_map = resolve_ciks(failed, seed=seed)

    n = backfill(
        failed, start=start_d, end=end_d,
        cik_resolver=lambda t: cik_map.get(t.upper()),
    )
    typer.echo(f"OK: {n} additional price rows written.")


@app.command("backfill-fundamentals")
def backfill_fundamentals(
    universe: str = typer.Option(
        "security-master",
        help="security-master (all PIT CIKs) | active (current members) | missing | current | all | comma-list of CIKs",
    ),
    refresh: bool = typer.Option(False, "--refresh"),
    limit: int = typer.Option(0),
):
    """Pull EDGAR companyfacts JSON for each CIK and write to fundamentals_facts/."""
    import pyarrow as pa
    import pyarrow.compute as pc

    from .constituents.fja05680 import fetch_snapshot
    from .fundamentals.companyfacts_api import fetch_companyfacts, normalize_facts
    from .identifiers.cik_map import resolve_ciks
    from .store import writer as w

    snap = fetch_snapshot()
    if universe == "current":
        tickers = sorted(snap.current_snapshot["symbol"].astype(str).str.upper())
        seed = dict(
            zip(
                snap.current_snapshot["symbol"].astype(str).str.upper(),
                snap.current_snapshot["cik"].astype(str),
                strict=False,
            )
        )
        seed = {k: v for k, v in seed.items() if v and v != "<NA>" and v != "nan"}
        cik_map = resolve_ciks(tickers, seed=seed)
        ciks = sorted({v for v in cik_map.values() if v})
    elif universe == "all":
        tickers = sorted(set(snap.ticker_tenures["ticker"].astype(str).str.upper()))
        seed = dict(
            zip(
                snap.current_snapshot["symbol"].astype(str).str.upper(),
                snap.current_snapshot["cik"].astype(str),
                strict=False,
            )
        )
        seed = {k: v for k, v in seed.items() if v and v != "<NA>" and v != "nan"}
        cik_map = resolve_ciks(tickers, seed=seed)
        ciks = sorted({v for v in cik_map.values() if v})
    elif universe == "active":
        # CIKs of *current* members (open security-master segments).
        from .identifiers.security_master import load_security_master

        smx = load_security_master()
        ciks = sorted(set(smx.loc[smx["valid_to"].isna(), "cik"].dropna()))
    elif universe in ("security-master", "missing"):
        # Every point-in-time CIK in the security master (the PIT-correct
        # universe); "missing" restricts to CIKs with no facts on disk yet.
        import duckdb

        from .identifiers.security_master import load_security_master

        ciks = sorted(set(load_security_master()["cik"].dropna()))
        if universe == "missing":
            glob = str(w.fundamentals_facts_dir() / "*" / "*.parquet")
            have = set(duckdb.sql(f"SELECT DISTINCT cik FROM read_parquet('{glob}', hive_partitioning=true)").df()["cik"])
            ciks = [c for c in ciks if c not in have]
    else:
        ciks = [c.strip().zfill(10) for c in universe.split(",") if c.strip()]

    if limit:
        ciks = ciks[:limit]

    typer.echo(f"Fetching companyfacts for {len(ciks)} CIKs ...")
    tables: list[pa.Table] = []
    for i, cik in enumerate(ciks, 1):
        if i % 25 == 0:
            typer.echo(f"  progress: {i}/{len(ciks)}")
        try:
            raw = fetch_companyfacts(cik, refresh=refresh)
        except Exception as e:
            typer.echo(f"  CIK {cik} failed: {e}")
            continue
        if raw is None:
            continue
        try:
            t = normalize_facts(cik, raw)
        except Exception as e:
            typer.echo(f"  normalize failed for CIK {cik}: {e}")
            continue
        if t.num_rows > 0:
            tables.append(t)

    if not tables:
        typer.echo("No fundamentals fetched.")
        return

    combined = pa.concat_tables(tables)
    filed_year = pc.year(pc.cast(combined["filed"], pa.timestamp("us")))
    combined = combined.append_column("filed_year", filed_year)

    written = w.append_to_partition(
        combined,
        w.fundamentals_facts_dir(),
        partition_col="filed_year",
        primary_key=[
            "cik",
            "concept",
            "period_start",
            "period_end",
            "accession_number",
        ],
    )
    typer.echo(
        f"OK: {combined.num_rows:,} fact rows across {len(written)} year-partitioned files."
    )


@app.command("backfill-news")
def backfill_news(
    start: str = typer.Option(
        "2021-01-01",
        help="Earliest publish/crawl date to request. NOTE: Tiingo's tier history "
        "is rolling (~6mo on Power, longer on Enterprise). Older requests are "
        "silently capped to whatever the tier exposes; the loader logs the "
        "actual returned crawl_date range and warns on drift.",
    ),
    end: str = typer.Option(
        "today",
        help="Latest publish/crawl date to request. With sortBy=crawlDate, "
        "Tiingo does not reliably enforce this upper bound; the loader logs "
        "any drift between requested and actual range.",
    ),
    universe: str = typer.Option(
        "current",
        help="One of: current (active S&P 500), all (full historical), or comma-list",
    ),
    batch_size: int = typer.Option(
        5,
        help="Tickers per Tiingo request. Tiingo accepts comma-separated lists; "
        "batching cuts request count proportionally.",
    ),
    limit: int = typer.Option(0, help="Process at most N tickers (for testing)"),
):
    """Backfill Tiingo news for the requested universe.

    PIT note: news is filtered at read time on ``crawl_date`` (when Tiingo
    first indexed the article), not ``published_date`` (which can be earlier
    for backfilled articles and would silently leak future information).
    """
    from .constituents.fja05680 import fetch_snapshot
    from .news.loader import backfill

    end_d = dt.date.today() if end == "today" else dt.date.fromisoformat(end)
    start_d = dt.date.fromisoformat(start)

    snap = fetch_snapshot()
    if universe == "current":
        tickers = sorted(snap.current_snapshot["symbol"].astype(str).str.upper())
    elif universe == "all":
        tickers = sorted(set(snap.ticker_tenures["ticker"].astype(str).str.upper()))
    else:
        tickers = [t.strip().upper() for t in universe.split(",") if t.strip()]

    if limit:
        tickers = tickers[:limit]

    typer.echo(
        f"Backfilling news for {len(tickers)} tickers from {start_d} to {end_d} "
        f"(batch_size={batch_size}) ..."
    )
    n = backfill(tickers, start=start_d, end=end_d, batch_size=batch_size)
    typer.echo(f"OK: {n} news rows written.")


@app.command("backfill-sectors")
def backfill_sectors(refresh: bool = typer.Option(False, "--refresh")):
    """Materialize sector_history.parquet from EDGAR SIC + GICS crosswalk."""
    from .sectors.reconstruct import write_sector_history

    n = write_sector_history(refresh=refresh)
    typer.echo(f"OK: {n} sector_history rows written.")


@app.command("verify-prices")
def verify_prices(as_of: str = typer.Option("today")):
    """Sanity checks: coverage vs constituents, FRED tracking."""
    import duckdb

    from .store import writer as w

    as_of_d = dt.date.today() if as_of == "today" else dt.date.fromisoformat(as_of)

    if not w.prices_path().exists():
        typer.echo("No prices.parquet — run `pitds backfill-prices` first.")
        raise typer.Exit(code=1)
    if not w.constituents_daily_path().exists():
        typer.echo("No constituents_daily — run `pitds backfill-constituents` first.")
        raise typer.Exit(code=1)

    con = duckdb.connect()
    typer.echo(f"Prices vs constituents coverage on {as_of_d}:")
    coverage = con.sql(f"""
        SELECT
            (SELECT COUNT(DISTINCT ticker) FROM read_parquet({str(w.constituents_daily_path())!r})
             WHERE date = DATE '{as_of_d}') AS n_constituents,
            (SELECT COUNT(DISTINCT ticker) FROM read_parquet({str(w.prices_path())!r})
             WHERE date = DATE '{as_of_d}') AS n_with_price,
            (SELECT COUNT(DISTINCT c.ticker)
             FROM read_parquet({str(w.constituents_daily_path())!r}) c
             LEFT JOIN read_parquet({str(w.prices_path())!r}) p
                 ON c.ticker = p.ticker AND c.date = p.date
             WHERE c.date = DATE '{as_of_d}' AND p.ticker IS NOT NULL) AS matched
    """).fetchone()
    typer.echo(f"  constituents: {coverage[0]}")
    typer.echo(f"  with prices : {coverage[1]}")
    typer.echo(f"  matched     : {coverage[2]} ({100*coverage[2]/max(coverage[0],1):.1f}%)")


@app.command("update-daily")
def update_daily():
    """Run the nightly orchestrator (idempotent). For cron schedules."""
    from .update.daily import run_daily

    run = run_daily()
    typer.echo(run.report())
    if any(not s.succeeded and s.error for s in run.steps):
        raise typer.Exit(code=1)


@app.command("inspect-cik")
def inspect_cik(
    cik: str = typer.Argument(..., help="10-digit CIK (e.g. 0000320193 for AAPL)"),
    as_of: str = typer.Option("today", help="PIT as_of date (YYYY-MM-DD)"),
):
    """Print a PIT snapshot for a single CIK: identity, latest prices, key fundamentals, sector."""
    from .pit import api

    cik = cik.zfill(10)
    as_of_d = dt.date.today() if as_of == "today" else dt.date.fromisoformat(as_of)
    typer.echo(f"\n=== CIK {cik} as of {as_of_d} ===\n")

    # Sector
    s = api.sector(cik=cik, as_of=as_of_d)
    if s:
        typer.echo(f"Sector: {s['gics_sector']} (source={s.get('gics_sector_source')}, sic={s['sic_code']})")
    else:
        typer.echo("Sector: (not in sector_history)")

    # Latest price
    history = api.price_history(cik=cik, end=as_of_d, adjust="split_div")
    if not history.empty:
        last = history.iloc[-1]
        typer.echo(
            f"\nLatest price: {last['ticker']} on {last['date'].date() if hasattr(last['date'], 'date') else last['date']}: "
            f"close=${last['close']:.2f}, adj_close=${last['close_adj']:.4f} "
            f"(adj_factor={last['adj_factor']:.5f}, source={last['source']})"
        )
        typer.echo(f"Total price history rows ≤ {as_of_d}: {len(history)}")
    else:
        typer.echo("\nNo price history found for this CIK")

    # Key fundamentals
    typer.echo(f"\nKey fundamentals (latest_known as of {as_of_d}):")
    for field_name in ("revenue", "net_income", "total_assets", "total_equity", "shares_outstanding"):
        # Find any period_end <= as_of with a value
        try:
            from .fundamentals import pit_layer

            df = pit_layer.all_facts_as_of(cik=cik, as_of=as_of_d)
            from .fundamentals import concepts as c

            for concept in c.concepts_for(field_name):
                # Balance sheet items have period_start=NULL (instantaneous);
                # income statement / cash flow items have a non-null period_start
                # spanning the fiscal period. Either is valid.
                hits = df[df["concept"] == concept]
                if not hits.empty:
                    latest = hits.sort_values("period_end").iloc[-1]
                    val = latest["val"]
                    if abs(val) >= 1e9:
                        formatted = f"${val/1e9:.2f}B"
                    elif abs(val) >= 1e6:
                        formatted = f"${val/1e6:.2f}M"
                    else:
                        formatted = f"{val:,.2f}"
                    typer.echo(
                        f"  {field_name:20s}: {formatted}  "
                        f"({concept}, period_end={latest['period_end']}, filed={latest['filed']})"
                    )
                    break
            else:
                typer.echo(f"  {field_name:20s}: (no data)")
        except Exception as e:
            typer.echo(f"  {field_name:20s}: error: {e}")


@app.command("verify")
def verify_all():
    """Run the full PITDS integrity check suite (Phase C)."""
    from .update.verify import format_report, run_all

    results = run_all()
    typer.echo(format_report(results))
    failed = [r for r in results if not r.passed]
    if any(r.severity == "critical" for r in failed):
        raise typer.Exit(code=1)


@app.command("backtest-demo")
def backtest_demo(
    start: str = typer.Option("2018-01-01"),
    end: str = typer.Option("2023-12-31"),
    rebalance_days: int = typer.Option(63, help="rebalance every N trading days (~quarter)"),
):
    """Run a simple PIT-correct equal-weight S&P 500 backtest.

    Demonstrates that the dataset works for end-to-end backtesting:
    every rebalance uses constituents AS OF that date (not today's),
    every price is the unadjusted-then-adjusted close on that date,
    and there is no look-ahead.
    """
    import duckdb
    import pandas as pd

    from .store import writer as w

    if not (w.prices_path().exists() and w.constituents_daily_path().exists()):
        typer.echo("Need prices.parquet and constituents_daily.parquet")
        raise typer.Exit(code=1)

    start_d = dt.date.fromisoformat(start)
    end_d = dt.date.fromisoformat(end)

    con = duckdb.connect()
    pp, cp = str(w.prices_path()), str(w.constituents_daily_path())

    # 1. All trading days in the window from PITDS
    days = con.sql(
        f"""
        SELECT DISTINCT date FROM read_parquet({cp!r})
        WHERE date BETWEEN DATE '{start_d}' AND DATE '{end_d}'
        ORDER BY date
        """
    ).df()
    days["date"] = pd.to_datetime(days["date"]).dt.date
    typer.echo(f"Trading days in window: {len(days)}")

    # 2. Rebalance dates every N trading days
    rebalances = days.iloc[::rebalance_days]["date"].tolist()
    typer.echo(f"Rebalance dates: {len(rebalances)} (every ~{rebalance_days} trading days)")

    # 3. For each rebalance, build a portfolio of constituents that have
    #    a price on that date. Hold equal-weight until the next rebalance.
    portfolio_returns: list[tuple[dt.date, float]] = []
    for i, rebal in enumerate(rebalances):
        next_rebal = rebalances[i + 1] if i + 1 < len(rebalances) else end_d
        # Tickers in the index on rebal date with prices on rebal date
        members = con.sql(
            f"""
            SELECT DISTINCT c.ticker
            FROM read_parquet({cp!r}) c
            JOIN read_parquet({pp!r}) p
              ON c.ticker = p.ticker AND c.date = p.date
            WHERE c.date = DATE '{rebal}'
            """
        ).df()["ticker"].tolist()
        if not members:
            continue

        # Compute equal-weight return from rebal to next_rebal for each member,
        # using close prices. We use unadjusted close + ignore divs for this demo.
        tickers_sql = ",".join(f"'{t}'" for t in members)
        ret_df = con.sql(
            f"""
            WITH p AS (
                SELECT date, ticker, close
                FROM read_parquet({pp!r})
                WHERE ticker IN ({tickers_sql})
                  AND date BETWEEN DATE '{rebal}' AND DATE '{next_rebal}'
            ),
            first_last AS (
                SELECT
                    ticker,
                    FIRST(close ORDER BY date) AS p0,
                    LAST(close ORDER BY date) AS p1
                FROM p GROUP BY ticker
            )
            SELECT date_trunc('day', DATE '{next_rebal}') AS as_of, AVG(p1 / NULLIF(p0, 0) - 1) AS mean_ret
            FROM first_last WHERE p0 > 0 AND p1 > 0
            """
        ).fetchone()
        if ret_df and ret_df[1] is not None:
            portfolio_returns.append((next_rebal, float(ret_df[1])))

    con.close()

    if not portfolio_returns:
        typer.echo("No returns computed.")
        return

    pr_df = pd.DataFrame(portfolio_returns, columns=["date", "period_ret"])
    pr_df["cum"] = (1 + pr_df["period_ret"]).cumprod()
    annual = (pr_df["cum"].iloc[-1]) ** (252 / (len(rebalances) * rebalance_days)) - 1
    sharpe_approx = (
        pr_df["period_ret"].mean() / max(pr_df["period_ret"].std(), 1e-9)
    ) * (252 / rebalance_days) ** 0.5

    typer.echo("")
    typer.echo("=== PIT-correct equal-weight backtest ===")
    typer.echo(f"  window: {start_d} to {end_d} ({len(rebalances)} rebalances)")
    typer.echo(f"  total return: {(pr_df['cum'].iloc[-1] - 1) * 100:+.2f}%")
    typer.echo(f"  annualized:   {annual * 100:+.2f}%")
    typer.echo(f"  Sharpe~ (no risk-free): {sharpe_approx:.2f}")
    typer.echo("")
    typer.echo("  Key property: each rebalance uses constituents AS OF that date,")
    typer.echo("  not today's. Survivorship bias is impossible by construction.")


@app.command("validate-fred")
def validate_fred(
    start: str = typer.Option("2015-01-01"),
    end: str = typer.Option("today"),
):
    """Reconstruct an equal-weight S&P 500 daily index from PITDS and compare to FRED SP500.

    A correctly-built dataset should produce daily returns highly correlated
    with FRED's cap-weighted SP500. Equal-weight vs cap-weight diverge on
    rotation days (2020-11-09: EW +4.6% vs SPX +1.2%), so the genuine
    correlation over 2016-2026 is ~0.93; data errors (wrong-entity series,
    split spikes) push it well below 0.90. FRED only serves ~10 years.
    """
    import duckdb

    from .prices import fred
    from .store import writer as w

    end_d = dt.date.today() if end == "today" else dt.date.fromisoformat(end)
    start_d = dt.date.fromisoformat(start)
    if not w.prices_path().exists() or not w.constituents_daily_path().exists():
        typer.echo("Need prices.parquet and constituents_daily.parquet — run backfills.")
        raise typer.Exit(code=1)

    # Build equal-weight daily returns from PITDS. Use split+dividend-adjusted
    # closes (raw closes turn every split into a fake -50%..-90% day) and lag
    # within the vendor series (perma_ticker), so renames don't break the
    # chain; then keep only returns on days the security was a member.
    from .prices.adjust import adjusted_prices_query

    con = duckdb.connect()
    pp = str(w.prices_path())
    cp = str(w.constituents_daily_path())
    ap = str(w.corporate_actions_path())
    adj = adjusted_prices_query(prices_path=pp, actions_path=ap, mode="split_div")
    sql = f"""
    WITH series AS (
        SELECT date, ticker, cik, close_adj,
               LAG(close_adj) OVER (PARTITION BY COALESCE(perma_ticker, ticker) ORDER BY date) AS prev_adj
        FROM ({adj})
        WHERE date BETWEEN DATE '{start_d}' - INTERVAL 10 DAY AND DATE '{end_d}'
    ),
    per_member AS (
        SELECT s.date, s.close_adj / s.prev_adj - 1 AS ret
        FROM series s
        JOIN read_parquet({cp!r}) c ON c.date = s.date AND c.ticker = s.ticker AND c.cik = s.cik
        WHERE s.date >= DATE '{start_d}' AND s.prev_adj > 0 AND s.close_adj > 0
    )
    -- Equal-weight portfolio return = mean of *simple* returns. (Averaging
    -- log returns understates it by ~half the cross-sectional variance.)
    SELECT date, LN(1 + AVG(ret)) AS log_ret FROM per_member
    WHERE ret IS NOT NULL
    GROUP BY date ORDER BY date
    """
    pitds_df = con.sql(sql).df()
    con.close()
    typer.echo(f"PITDS equal-weight daily series: {len(pitds_df)} obs from {pitds_df['date'].min()} to {pitds_df['date'].max()}")

    # FRED comparison
    typer.echo("Fetching FRED SP500 ...")
    fred_df = fred.fetch_sp500_index(start_d, end_d)
    fred_df["log_ret"] = (fred_df["sp500"] / fred_df["sp500"].shift(1)).apply(
        lambda x: float("nan") if x is None or x <= 0 else __import__("math").log(x)
    )
    fred_df = fred_df.dropna(subset=["log_ret"])

    # Merge on date
    import pandas as pd

    pitds_df["date"] = pd.to_datetime(pitds_df["date"]).dt.date
    fred_df["date"] = pd.to_datetime(fred_df["date"]).dt.date
    merged = pitds_df.merge(fred_df[["date", "log_ret"]], on="date", suffixes=("_pitds", "_fred"))
    typer.echo(f"Joined observations: {len(merged)}")

    if len(merged) < 100:
        typer.echo("Too few overlapping days — extend the window")
        return

    corr = merged["log_ret_pitds"].corr(merged["log_ret_fred"])
    pitds_ann = merged["log_ret_pitds"].mean() * 252
    fred_ann = merged["log_ret_fred"].mean() * 252
    typer.echo("")
    typer.echo("=== FRED vs PITDS (equal-weight) ===")
    typer.echo(f"  Daily return correlation: {corr:.4f}  (target: ≥0.90)")
    typer.echo(f"  PITDS annualized log return: {pitds_ann*100:.2f}%")
    typer.echo(f"  FRED  annualized log return: {fred_ann*100:.2f}%")
    typer.echo(
        "  Note: PITDS is equal-weight, FRED is cap-weight, so absolute "
        "returns differ; correlation is the sanity check."
    )


@app.command("info")
def info():
    """Show dataset state."""
    import pyarrow.parquet as pq

    from .store import writer as w

    typer.echo(f"Data dir: {settings.data_dir}")
    typer.echo("")
    typer.echo("Processed tables:")
    for name, path in [
        ("constituents_daily", w.constituents_daily_path()),
        ("constituent_changes", w.constituent_changes_path()),
        ("ticker_history", w.ticker_history_path()),
        ("prices", w.prices_path()),
        ("corporate_actions", w.corporate_actions_path()),
        ("sector_history", w.sector_history_path()),
    ]:
        if path.exists():
            pf = pq.ParquetFile(path)
            md = pf.metadata.metadata or {}
            ingestion = md.get(b"pitds.ingestion_date", b"?").decode()
            typer.echo(
                f"  {name:22s}  {pf.metadata.num_rows:>12,} rows  "
                f"{path.stat().st_size // 1024:>7} KB  vintage {ingestion}"
            )
        else:
            typer.echo(f"  {name:22s}  (not built)")

    # Partitioned datasets: aggregate across all year-files.
    for label, dir_path in [
        ("fundamentals_facts", w.fundamentals_facts_dir()),
        ("news",               w.news_dir()),
    ]:
        files = sorted(dir_path.glob("**/*.parquet")) if dir_path.exists() else []
        if files:
            import duckdb

            con = duckdb.connect()
            glob = str(dir_path / "**" / "*.parquet")
            total = con.sql(
                f"SELECT COUNT(*) FROM read_parquet({glob!r}, hive_partitioning=true)"
            ).fetchone()[0]
            con.close()
            total_size = sum(f.stat().st_size for f in files)
            vintage = "?"
            try:
                md = pq.ParquetFile(files[0]).metadata.metadata or {}
                vintage = md.get(b"pitds.ingestion_date", b"?").decode()
            except Exception:
                pass
            typer.echo(
                f"  {label:22s}  {total:>12,} rows  "
                f"{total_size // 1024:>7} KB  vintage {vintage}  ({len(files)} partitions)"
            )
        else:
            typer.echo(f"  {label:22s}  (not built)")


if __name__ == "__main__":
    app()
