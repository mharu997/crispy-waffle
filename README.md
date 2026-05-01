# PITDS — Point-In-Time S&P 500 Dataset

A survivorship-bias-free, point-in-time dataset of S&P 500 constituents, prices, corporate actions, fundamentals, and sector classifications, built from **free data sources only** and covering **2010-01-01 to today**.

## What this dataset is

- **Point-in-time (PIT)**: every query takes an `as_of` date and returns only data that was knowable on that date. No survivorship bias, no look-ahead bias from restated fundamentals, no index-inclusion bias.
- **Reproducible**: unadjusted prices stored with a corporate-action log; adjustments computed on read. Same inputs → same outputs forever.
- **Append-only**: historical rows are never overwritten. Restatements appear as new rows tagged by SEC accession number.
- **Self-updating**: a nightly job keeps the dataset PIT going forward.

## What this dataset is NOT

- **No data before 2010.** Pre-XBRL fundamentals are not freely available in PIT form.
- **No GICS Industry/Sub-Industry historically** — only Sector. SIC code (PIT-correct from EDGAR) is the rigorous fallback.
- **No CRSP-style delisting returns.** Delisted-due-to-bankruptcy tickers are flagged but the last-month return underestimates true loss.
- **No intraday data.** EOD only.

## Quickstart

### 1. Set up Python environment

```bash
# From the project root
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Configure API keys

```bash
cp .env.example .env
# Edit .env and fill in:
# - TIINGO_API_KEY (free, from https://www.tiingo.com/)
# - SEC_USER_AGENT (your name and email — required by EDGAR)
# - OPENFIGI_API_KEY (optional, free, lifts the rate limit)
```

### 3. Backfill (full Phase A+B: ~1 hour wall time)

```bash
# Phase A — universe, prices, corporate actions
pitds backfill-constituents
pitds backfill-identifiers
pitds backfill-prices --universe current --start 2010-01-01
pitds backfill-corporate-actions --universe current
# optional: also pull the full historical universe (delisted tickers; many will fail)
pitds backfill-prices --universe all --start 2010-01-01

# Phase B — fundamentals + sectors
pitds backfill-fundamentals --universe current
pitds backfill-sectors

# Verify
pitds verify
pitds validate-fred --start 2018-01-01
```

### 4. Query the PIT API

```python
from pitds.pit import api
from datetime import date

# Universe (rounds non-trading days down to previous trading day)
universe = api.constituents(as_of=date(2018, 3, 15))

# Adjusted prices (split + dividend, computed on read)
df = api.prices(as_of=date(2024, 6, 28), ticker='AAPL', adjust='split_div')

# Vintage-aware fundamentals
revenue = api.fact(cik='0000320193', field_name='revenue',
                   period_end=date(2024, 3, 30),
                   as_of=date(2024, 6, 28))               # latest_known
revenue_first = api.fact(cik='0000320193', field_name='revenue',
                         period_end=date(2024, 3, 30),
                         as_of=date(2024, 6, 28),
                         vintage='first_reported')         # as originally reported

# Sector (Wikipedia-observed preferred over SIC crosswalk)
s = api.sector(cik='0000320193', as_of=date(2024, 6, 28))
```

### 5. Built-in CLI commands

```bash
pitds info                    # dataset state with vintage stamps
pitds inspect-cik 0000320193  # PIT snapshot for one CIK
pitds verify                  # 7 integrity + regression checks
pitds validate-fred           # daily-return correlation vs FRED SP500
pitds backtest-demo           # PIT-correct equal-weight backtest
pitds update-daily            # nightly orchestrator (cron-friendly)
pitds retry-failed-prices     # recover tickers that hit dot/rate-limit issues
```

## Architecture

See the implementation plan at `~/.claude/plans/i-want-to-put-fluffy-lovelace.md` and the code under `src/pitds/`.

Storage: Parquet files under `data/processed/`, queried via DuckDB. CIK is the canonical join key.

## Data sources

| Layer | Primary | Fallback |
|---|---|---|
| Constituents | `fja05680/sp500` (GitHub, monthly updates) | Wikipedia revision history |
| Prices/volume | Tiingo free tier | yfinance, Stooq bulk |
| Corporate actions | Tiingo | yfinance |
| Fundamentals (PIT) | SEC EDGAR `companyfacts` JSON | `edgartools` wrapper |
| Sectors (PIT) | SIC from EDGAR filing headers | Wikipedia revision history |
| Identifiers | OpenFIGI | EDGAR `company-tickers-exchange.json` |
| Trading calendar | `pandas-market-calendars` NYSE | — |
| Index validation | FRED `SP500` | — |

## Status

- [x] Plan finalized
- [x] Project skeleton + Arrow schemas
- [x] Phase A: constituents + identifiers + prices + corporate actions
- [x] Phase B: fundamentals (XBRL companyfacts loader + vintage-aware PIT layer + concept aliasing)
- [x] Phase B: sectors (SIC from EDGAR + GICS crosswalk + Wikipedia-observed)
- [x] Phase C: daily updater + 7-check integrity suite + 9 invariant tests
- [x] FRED tracking validation (≥0.95 daily-return correlation)

## Known limitations

- **Pre-2010 fundamentals**: not freely PIT (XBRL mandate started 2009).
- **Historical-only ticker price coverage**: ~85-90% via free sources; remaining ~10-15% need OpenFIGI integration (planned Phase B refinement).
- **GICS Industry / Sub-Industry**: not historically reconstructable for free; only Sector available.
- **Cap-weighting**: requires `shares_outstanding` from PITDS fundamentals (PIT-aware) — works, but yields slight differences vs FRED's daily-cap due to free-tier filing-frequency limits.
- **Constituent-change effective dates**: 1-day off-by-one in fja05680 source for some events (e.g., GE removal); flagged as warning by `pitds verify`.
