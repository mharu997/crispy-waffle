# PITDS — Point-In-Time S&P 500 Dataset

A survivorship-bias-free, point-in-time dataset of S&P 500 constituents, prices, corporate actions, fundamentals, and sector classifications, built from **free data sources only** and covering **2010-01-01 to today**.

## What this dataset is

- **Point-in-time (PIT)**: every query takes an `as_of` date and returns only data that was knowable on that date. No survivorship bias, no look-ahead bias from restated fundamentals, no index-inclusion bias.
- **Reproducible**: unadjusted prices stored with a corporate-action log; adjustments computed on read. Same inputs → same outputs forever.
- **Append-only**: historical rows are never overwritten. Restatements appear as new rows tagged by SEC accession number.
- **Self-updating**: a nightly job keeps the dataset PIT going forward.

## Identity: every row is tied to the right company

Tickers get renamed (PCLN → BKNG), reused (STI: SunTrust → Solidion) and
reorganized (Google Inc → Alphabet). Price vendors resolve a ticker to whoever
holds it *today*, so a ticker-keyed pipeline silently attaches the wrong
company's history. PITDS therefore builds a **security master** first
(`data/processed/security_master.parquet`) mapping every post-2010 membership
segment to:

| field | source |
|---|---|
| `ticker_pit` — ticker *as of the date* | fja05680 label, corrected from dated Wikipedia snapshots (revision bisection for exact switch dates) |
| `cik` — SEC entity | Wikipedia snapshot CIKs, validated against EDGAR filing activity + name history; EDGAR name search before 2014 |
| `perma_ticker` — vendor price series | Tiingo permaTicker (pins one company across renames/reuse), chosen by trading-date coverage |

Prices **and** dividends/splits are fetched from the same permaTicker series,
adjustments are partitioned by that series, and every member-day carries a CIK.
Hand-verified exceptions live in version-controlled CSVs, each with a reason:
`identifiers/data/entity_overrides.csv`, `constituents/data/membership_corrections.csv`,
`identifiers/data/ticker_labels.csv`, `corporate_actions/data/action_corrections.csv`.

**Live edge.** fja05680 lags S&P announcements by weeks to months. Membership
changes newer than the fja snapshot are taken from Wikipedia's current
revision and dated by revision bisection (±1 day), tagged
`wikipedia_live_edge` in `data/processed/live_edge_corrections.parquet`, and
superseded automatically once fja catches up.

### Coverage (as of 2026-11-07)

| | 2010 | 2012 | 2014 | 2016 | 2018 | 2020–2026 |
|---|---|---|---|---|---|---|
| member-days with a CIK | 100% | 100% | 100% | 100% | 100% | 100% |
| member-days with a (same-CIK) price | 90.8% | 93.3% | 95.8% | 98.7% | 99.6% | 99.6–100% |
| member-days with XBRL facts filed by that date | 83.5% | 99.4% | 99.5% | 99.6% | 99.9% | 99–99.8% |

## What this dataset is NOT

- **No data before 2010.** Pre-XBRL fundamentals are not freely available in PIT form.
- **Prices missing for ~45 companies that left the index before 2016** (e.g. Dow Chemical, Goodrich, BMC, Molex, Progress Energy, Dell Inc, old Chubb, old Johnson Controls, old Allergan). Tiingo has no series for most entities delisted before 2016. These segments carry `price_status ∈ {unresolved, no_source, low_coverage}` in the security master — the gap is explicit, not silent. Backtests on 2010–2015 retain mild survivorship tilt.
- **EDGAR companyfacts lag for ~30 combined filers.** Since spring 2026 the SEC companyfacts API omits recent 10-Q facts for several utilities/REITs (filers with co-registrant subsidiaries/OPs) and Citigroup. Facts are as complete as the API serves them.
- **Sector history is observed quarterly** (Wikipedia snapshots), not at the exact reclassification date. SIC code (EDGAR) remains the rigorous fallback.
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
# Identity spine → prices, corporate actions, constituents, sectors (one command)
pitds build-security-master --refresh-wiki --refresh-meta

# Fundamentals for every point-in-time CIK
pitds backfill-fundamentals --universe security-master

# Phase D — news enrichment (Tiingo Power tier exposes ~3-6mo rolling window)
pitds backfill-news --universe all --start 2010-01-01

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
pitds build-security-master   # rebuild identity spine + everything keyed on it
pitds rebuild-prices          # prices + actions from the security master's series
pitds verify                  # 12 integrity + regression checks
pitds validate-fred           # daily-return correlation vs FRED SP500
pitds backtest-demo           # PIT-correct equal-weight backtest
pitds update-daily            # nightly orchestrator (cron-friendly)
pitds backfill-news           # Tiingo News for the historical universe
pitds retry-failed-prices     # recover tickers that hit dot/rate-limit issues
```

## Using PITDS from another project

### What ships with the package vs what doesn't

The repo / pip install ships **only the code** (~50 source/doc files, ~5 MB). It does **NOT** ship the actual dataset:

| Shipped in git / pip | NOT shipped (gitignored, lives only on your canonical install) |
|---|---|
| `src/pitds/` (the Python package) | `data/processed/prices.parquet` (~46 MB) |
| `pyproject.toml`, `README.md`, `DESIGN.md` | `data/processed/fundamentals_facts/` (~94 MB) |
| `tests/`, `scripts/` (launchd plist + wrapper) | `data/processed/{constituents_daily,corporate_actions,sector_history,ticker_history}.parquet` |
| `.env.example` (template only) | `data/raw/edgar/companyfacts/CIK*.json` (~2.6 GB raw cache) |
| | `.env` (Tiingo key, SEC user-agent) |

This separation is deliberate:
1. **Size** — 142 MB of Parquet (plus ~2.6 GB raw cache) doesn't belong in git
2. **Vintage** — the data updates daily; you'd need a new commit per snapshot
3. **Licensing** — Tiingo's TOS likely prohibit redistributing their data, even in a private repo
4. **Reproducibility** — code and data should version independently (pin code v1.2 + data vintage 2026-04-30)

So PITDS is **a library + an on-disk data store**. The library knows *how* to fetch and query the data; the data lives once on the canonical install (this Mac), populated by `pitds backfill-*` commands and refreshed nightly by the launchd agent.

### Three patterns for sharing the data

```
        same Mac, multiple projects                    cross-machine / cloud

┌──────────────────────────────────────────┐  ┌────────────────────────────────────┐
│ A.  PITDS_DATA_DIR                       │  │ B.  Each project backfills its own │
│     (recommended for local work)         │  │     copy                           │
│                                          │  │                                    │
│  ✓ 1 copy of data on disk                │  │  ✗ 142 MB × N projects             │
│  ✓ launchd refresh updates everyone      │  │  ✗ N Tiingo API calls per day      │
│  ✓ consumers need NO Tiingo key, NO      │  │  ✓ each project independent        │
│    SEC user-agent (read-only)            │  │  ✓ projects can pin different      │
│                                          │  │    vintages                        │
└──────────────────────────────────────────┘  └────────────────────────────────────┘

         ┌─────────────────────────────────────────────────────┐
         │ C.  Cloud snapshot (S3 / DVC / git LFS)             │
         │   - sync data/processed/ to S3 after each update    │
         │   - consumers pull a specific snapshot when needed  │
         │   - works across machines, CI, collaborators        │
         │   - more setup; same Tiingo TOS caution applies     │
         └─────────────────────────────────────────────────────┘
```

For local work on this Mac with multiple sibling projects, **Option A is the right answer**.

### Option A: install + read-only consumer (typical)

In the consuming project (e.g. `AITrade`, `KalshiProject`, `rlmodel1`):

```bash
cd /path/to/your/other-project
source .venv/bin/activate            # the consuming project's venv

# Install the PITDS package — pick ONE
pip install -e /Users/mharutyunyan/Projects/Enso/PITDS                  # local editable
pip install git+https://github.com/mharu997/crispy-waffle.git@main      # from private GitHub

# Tell PITDS where the canonical data lives. This is the ONLY required env var
# for read-only consumers.
echo 'PITDS_DATA_DIR=/Users/mharutyunyan/Projects/Enso/PITDS/data' >> .env
```

Then in the consuming project's Python code:

```python
from pitds.pit import api
from datetime import date

universe = api.constituents(as_of=date(2024, 6, 28))
prices   = api.prices(as_of=date(2024, 6, 28), ticker='AAPL', adjust='split_div')
revenue  = api.fact(cik='0000320193', field_name='revenue',
                    period_end=date(2024, 3, 30),
                    as_of=date(2024, 6, 28))
sector   = api.sector(cik='0000320193', as_of=date(2024, 6, 28))
```

That's it. The consuming project is read-only — it never writes, never fetches anything from Tiingo or EDGAR. The launchd agent on the canonical install keeps the shared data fresh.

#### When you DO need API keys in a consumer

You only need to add `SEC_USER_AGENT` and `TIINGO_API_KEY` to a consumer project's `.env` if **that project itself runs backfills** (uncommon — usually you don't want this; the canonical install is the single producer). If the consumer ever calls `pitds.fundamentals.companyfacts_api.fetch_companyfacts()` for a CIK that isn't already cached, EDGAR needs `SEC_USER_AGENT`. For pure `api.*` queries against pre-backfilled data, no keys needed.

#### Programmatic override (instead of `.env`)

If the consumer doesn't use a `.env` file:

```python
import os
os.environ["PITDS_DATA_DIR"] = "/Users/mharutyunyan/Projects/Enso/PITDS/data"
from pitds.pit import api    # import AFTER setting the env var
```

### Option C: cloud snapshots for cross-machine reproducibility

For published backtests or multi-machine work:

```bash
# After a successful update on the canonical install:
aws s3 sync data/processed/ s3://your-bucket/pitds/$(date +%Y-%m-%d)/

# Each consumer (different machine, CI, etc.) pulls a specific vintage:
aws s3 sync s3://your-bucket/pitds/2026-04-30/ /tmp/pitds-data/
export PITDS_DATA_DIR=/tmp/pitds-data
```

The `pitds.ingestion_date` Parquet metadata in each file records which vintage you're using; `pitds info` displays it. Snapshot whenever you want a stable point of reference.

### Three rules to remember

1. **Only the canonical install runs `pitds update-daily`.** The launchd agent handles this. Consumers should never run backfills, or they'll write conflicting data.
2. **`PITDS_DATA_DIR` is how everyone shares the data.** It's the only required env var in a read-only consumer's `.env`.
3. **For published-backtest reproducibility**, snapshot `data/processed/` to S3 / DVC / git LFS at the time of the run, and pin consumers to that snapshot path.

## Architecture

See `DESIGN.md` for the comprehensive operational doc, or the implementation plan at `~/.claude/plans/i-want-to-put-fluffy-lovelace.md`.

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
| **News (PIT)** | **Tiingo News API** | — |

## Status

- [x] Plan finalized
- [x] Project skeleton + Arrow schemas
- [x] Phase A: constituents + identifiers + prices + corporate actions
- [x] Phase B: fundamentals (XBRL companyfacts loader + vintage-aware PIT layer + concept aliasing)
- [x] Phase B: sectors (SIC from EDGAR + GICS crosswalk + Wikipedia-observed)
- [x] Phase C: daily updater + 7-check integrity suite + 9 invariant tests
- [x] FRED tracking validation (≥0.95 daily-return correlation)
- [x] Phase D: news enrichment (Tiingo News API, hive-partitioned by `crawl_year`, daily refresh integrated 7-day/week, see `DESIGN.md` §16)

## Known limitations

- **Pre-2010 fundamentals**: not freely PIT (XBRL mandate started 2009).
- **Historical-only ticker price coverage**: ~85-90% via free sources; remaining ~10-15% need OpenFIGI integration (planned Phase B refinement).
- **GICS Industry / Sub-Industry**: not historically reconstructable for free; only Sector available.
- **Cap-weighting**: requires `shares_outstanding` from PITDS fundamentals (PIT-aware) — works, but yields slight differences vs FRED's daily-cap due to free-tier filing-frequency limits.
- **Constituent-change effective dates**: 1-day off-by-one in fja05680 source for some events (e.g., GE removal); flagged as warning by `pitds verify`.
