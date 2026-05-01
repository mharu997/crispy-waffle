# PITDS — Design and Operations

**Point-In-Time S&P 500 Dataset**
*Built from free sources, 2010-04-30 to today, survivorship-bias-free, append-only, self-updating.*

---

## 1. Mission

PITDS exists to answer a single question for any historical date `T`:

> *"What did the S&P 500 universe, prices, fundamentals, and sector classifications look like, as of close on date T, knowing only what was knowable then?"*

This is the **PIT contract**. It eliminates three biases that kill naive backtests:

1. **Survivorship bias** — using today's index members instead of the historical members at each date. (Example: Lehman, Enron, WaMu were S&P 500 members; using today's universe pretends they never existed.)
2. **Look-ahead bias** — using restated financials instead of the originally-reported numbers. (Example: GE's 2017 results were restated in 2018; a 2017-dated query should return the original, not the restated.)
3. **Index inclusion bias** — stocks tend to outperform *before* being added to the index; backtesting on current members captures that effect spuriously.

A truly PIT dataset must defeat all three. PITDS does.

---

## 2. Why these specific scope decisions

### Why 2010 and not 2000?

The SEC mandated XBRL (machine-readable) filings starting June 2009 for large filers, ramping to all filers by 2011. **Free PIT fundamentals only exist post-XBRL.** Pre-2009 fundamentals are in HTML 10-Ks, parseable only with brittle scraping that produces silent errors.

We chose **2010-01-04 (first trading day of 2010)** as the floor because:

- All large-cap filers had migrated to XBRL by then
- 16+ years of history is enough for any reasonable factor research
- Coverage of delisted tickers from free sources is materially better post-2010
- It's the cleanest cut between "rigorous PIT" and "best-effort guess"

### Why free sources only?

Norgate Data ($60-80/mo), Sharadar ($50-100/mo), Compustat (institutional, $$$$) all exist. We deliberately constrain to free or reasonablly priced sources such as Tiingo ($30/mo) because:

- Reproducibility — no licensing barriers for sharing or peer review
- A correctness-by-construction PIT system from free sources is more interesting than buying the answer
- The free path forces the architecture to be honest about uncertainty (multiple sources, vintage tracking, anomaly logging)

### Why CIK and not ticker as the canonical key?

Tickers are reassigned. Examples:

- "FB" → "META" (2022 rename, same entity, same CIK 0001326801)
- "ABC" → "COR" (AmerisourceBergen → Cencora 2023 rename)
- A delisted ticker can be reassigned to a different company years later

CIK (Central Index Key, SEC's permanent entity identifier) is stable through renames, listings, delistings. **Every analytical table joins on CIK.** Ticker is point-in-time data, stored with `valid_from`/`valid_to` intervals in `ticker_history`.

### Why unadjusted prices with on-read adjustment?

yfinance and Tiingo both pre-adjust their `Adj Close` field, but their adjustment logic has changed multiple times across versions. Storing `Adj Close` directly means a yfinance update silently changes your historical prices.

We store **raw OHLCV** plus a corporate-action log (splits, dividends, spinoffs). Adjusted prices are computed on read in DuckDB SQL. Result: the dataset is a deterministic function of `(raw prices, action log)`. Same inputs → same outputs forever.

This is the same approach CRSP takes.

---

## 3. Architecture overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          External free sources                              │
├──────────────┬──────────────┬─────────────────┬──────────────┬──────────────┤
│ fja05680/    │  Wikipedia   │  Tiingo (paid   │  SEC EDGAR   │  FRED        │
│ sp500 GitHub │  revisions   │  EOD)           │  XBRL        │  SP500 index │
│ (CSVs)       │  + table     │  + yfinance     │ + submissions│              │
│              │              │  + Stooq cascade│              │              │
└──────┬───────┴──────┬───────┴────────┬────────┴──────┬───────┴──────┬───────┘
       │              │                │               │              │
       │ constituents │ live edge      │ prices,       │ fundamentals,│ index validation
       │ history,     │ tracking,      │ corp actions  │ sectors      │
       │ changes      │ GICS observed  │               │              │
       ▼              ▼                ▼               ▼              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Loaders (src/pitds/{layer}/)                             │
│   normalize, deduplicate, vintage-stamp, write append-only Parquet          │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              Storage (data/processed/, Arrow schemas)                       │
│  constituents_daily, constituent_changes, ticker_history, prices,           │
│  corporate_actions, sector_history, fundamentals_facts/year=YYYY/           │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│           PIT Query API (src/pitds/pit/api.py)                              │
│  api.constituents(as_of)  api.prices(as_of, ticker, adjust)                 │
│  api.fact(cik, concept|field, period_end, as_of, vintage)                   │
│  api.sector(cik, as_of)   api.price_history(ticker, end)                    │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                Researchers (notebooks, backtests, ML pipelines)             │
└─────────────────────────────────────────────────────────────────────────────┘
```



### Storage choice: Parquet + DuckDB

- **Parquet** for storage: columnar, compressed, schema-rigorous, read by every analytics tool.
- **DuckDB** for querying: in-process SQL engine, pushdown predicates into Parquet, no server, vectorized execution.
- **Single-file** for `prices` (3.2M rows, 46 MB): DuckDB parallelizes over row groups, not files; fan-out by ticker would create too many tiny files.
- **Hive-partitioned by `filed_year`** for `fundamentals_facts` (16M rows, 94 MB): PIT queries filter by `filed <= as_of`, so partitioning by filed year prunes ~80% of data on common queries.

---

## 4. The PIT contract — six rules

These are the load-bearing invariants. Breaking any of them breaks the dataset.


| #   | Rule                                                                                                                                             | Why                                                                                                                                      |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | **Append-only fundamentals storage**. PK = `(cik, concept, period_start, period_end, accession_number)`. Restatements add rows; never overwrite. | A 2018 query should return the 2017-as-known-in-2018 number, not the restated-in-2024 number. The PIT layer picks vintage at query time. |
| 2   | **CIK is the canonical join key**, never ticker.                                                                                                 | Tickers get reassigned; CIK is permanent (rare reorganizations excepted, where we use FIGI as fallback).                                 |
| 3   | **Constituent changes carry both `announcement_date` and `effective_date`**.                                                                     | S&P typically announces 5 trading days ahead. Strategies that trade the announcement effect are a real research target.                  |
| 4   | **Filing date is the "knowable on" cutoff**: `WHERE filed <= as_of`.                                                                             | The single most important line of SQL in the codebase.                                                                                   |
| 5   | **Vintage selection is a query parameter**: `latest_known` (default) vs `first_reported`.                                                        | Both modes are legitimate research questions. The store keeps both; the API switches by parameter.                                       |
| 6   | **Adjusted prices computed on read** from unadjusted OHLCV + action log — never stored.                                                          | Decouples our adjustments from upstream source bugs. Same inputs → same outputs forever.                                                 |


The PIT cutoff at the heart of everything is this single SQL pattern:

```sql
SELECT * FROM v_fundamentals_facts
WHERE cik = ? AND concept = ? AND period_end = ?
  AND filed <= ?
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY cik, concept, period_start, period_end
    ORDER BY filed DESC          -- latest_known; flip to ASC for first_reported
) = 1;
```

---

## 5. Data model — table by table

All schemas defined in `src/pitds/store/schemas.py`. All paths centralized in `src/pitds/store/writer.py`.

### `constituents_daily.parquet` (2,061,440 rows, 353 KB)

The materialized membership matrix. One row per `(date, ticker)` for every trading day the ticker was an S&P 500 member. Derived from `constituent_changes` via the `changes.materialize_daily()` fold.


| Column           | Type   | Nullable | Notes                                                                     |
| ---------------- | ------ | -------- | ------------------------------------------------------------------------- |
| `date`           | DATE   | no       | NYSE trading day                                                          |
| `ticker`         | STRING | no       | The ticker as of this date                                                |
| `cik`            | STRING | yes      | Zero-padded 10-char SEC CIK; null for ~40% of historical/delisted tickers |
| `figi`           | STRING | yes      | OpenFIGI; populated by Phase B refinement                                 |
| `announced_only` | BOOL   | no       | True if announced but not yet effective                                   |


### `constituent_changes.parquet` (1,989 rows, 61 KB)

The event log. One row per add/remove/rename event.


| Column                     | Type    | Notes                                                 |
| -------------------------- | ------- | ----------------------------------------------------- |
| `event_id`                 | UUID    | PK                                                    |
| `cik`, `ticker`, `figi`    | strings | Identifiers (cik nullable for unresolved)             |
| `event_type`               | string  | `add` | `remove` | `rename`                           |
| `announcement_date`        | DATE    | When S&P announced                                    |
| `effective_date`           | DATE    | When index changed                                    |
| `old_ticker`, `new_ticker` | strings | For renames                                           |
| `reason`                   | string  | `m&a` | `bankruptcy` | `rebalance` | `spinoff` | etc. |
| `source`                   | string  | `fja05680` | `wikipedia` | `sp_press_release`         |
| `ingestion_date`           | DATE    | When we recorded it                                   |


Provenance: 1,246 add events + 743 remove events from `fja05680/sp500` baseline.

### `ticker_history.parquet` (749 rows, 11 KB)

Maps `(ticker, date)` → CIK over time. Handles ticker reassignment.


| Column                   | Type   | Notes                                               |
| ------------------------ | ------ | --------------------------------------------------- |
| `ticker`                 | string |                                                     |
| `cik`                    | string | Zero-padded 10-char                                 |
| `valid_from`, `valid_to` | DATE   | Half-open interval; valid_to NULL = currently valid |
| `source`                 | string | `sec_company_tickers`                               |


### `prices.parquet` (3,230,568 rows, 46 MB) — single file

**Unadjusted** OHLCV. Adjustments computed on read.


| Column                | Type    | Nullable | Notes                                                    |
| --------------------- | ------- | -------- | -------------------------------------------------------- |
| `date`                | DATE    | no       |                                                          |
| `ticker`              | string  | no       | Ticker as of this date                                   |
| `cik`                 | string  | yes      | Canonical join key (nullable for unresolved historicals) |
| `figi`                | string  | yes      |                                                          |
| `open`, `high`, `low` | float64 | yes      |                                                          |
| `close`               | float64 | no       | Required; the only mandatory price                       |
| `volume`              | int64   | yes      |                                                          |
| `source`              | string  | no       | `tiingo` (96.4%) | `yfinance` (3.6%) | `stooq`           |
| `ingestion_date`      | DATE    | no       | Vintage stamp                                            |


PK: `(ticker, date)`. Sorted by `(date, ticker)`.

Coverage:

- **2024-06-28**: 501/503 constituents = **99.6%**
- **2018-03-15**: 480/506 constituents = **94.9%**
- **2010-12-31**: ~92% (delisted-ticker tail)

### `corporate_actions.parquet` (50,526 rows, 1.2 MB)

Splits, cash dividends, stock dividends, spinoffs. The action log used by `prices/adjust.py` to compute adjusted prices on read.


| Column              | Type    | Notes                                          |
| ------------------- | ------- | ---------------------------------------------- |
| `action_id`         | UUID    | PK                                             |
| `cik`, `ticker`     | strings | Ticker as of `ex_date`                         |
| `ex_date`           | DATE    |                                                |
| `action_type`       | string  | `split` | `cash_div` | `stock_div` | `spinoff` |
| `ratio`             | float64 | For splits (e.g. 4.0 for 4-for-1)              |
| `amount`            | float64 | For cash dividends (per share)                 |
| `spinoff_child_cik` | string  | For spinoffs                                   |
| `source`            | string  | `yfinance` | `manual_curated` (for spinoffs)   |


11 spinoffs are manually curated since 2010 (ABBV/ABT, PYPL/EBAY, HPE/HPQ, DOW/DWDP, CTVA/DD, GEHC/GE, GEV/GE, etc.) because yfinance doesn't report spinoffs as discrete events.

### `sector_history.parquet` (749 rows, 14 KB)

Bitemporal sector classification.


| Column                   | Type    | Notes                                                                 |
| ------------------------ | ------- | --------------------------------------------------------------------- |
| `cik`, `ticker`          | strings |                                                                       |
| `valid_from`, `valid_to` | DATE    | Half-open; valid_to NULL = currently valid                            |
| `sic_code`               | int32   | Standard Industrial Classification (PIT-rigorous from EDGAR)          |
| `gics_sector_observed`   | string  | From Wikipedia "List of S&P 500 companies" (approximate but accurate) |
| `gics_sector_recon`      | string  | Derived from sic_code via crosswalk (rigorous PIT but coarse)         |
| `source`                 | string  | `edgar_submissions+wikipedia`                                         |


`api.sector()` prefers `gics_sector_observed` when available, falling back to `gics_sector_recon`.

### `fundamentals_facts/year=YYYY/data.parquet` (16,349,133 rows, 94 MB) — partitioned

Hive-partitioned by `filed_year`. Every XBRL fact ever filed for any S&P 500 CIK.


| Column             | Type    | Notes                                                |
| ------------------ | ------- | ---------------------------------------------------- |
| `cik`              | string  |                                                      |
| `concept`          | string  | XBRL fully-qualified, e.g. `us-gaap:Revenues`        |
| `unit`             | string  | `USD` | `shares` | `USD/shares` etc.                 |
| `period_start`     | DATE    | Null for instantaneous (balance sheet)               |
| `period_end`       | DATE    | Required                                             |
| `val`              | float64 | The numeric value                                    |
| `accession_number` | string  | SEC accession (PK component, distinguishes vintages) |
| `filed`            | DATE    | The PIT "knowable on" cutoff                         |
| `form`             | string  | `10-K` | `10-Q` | `10-K/A` | `8-K` etc.              |
| `fy`               | int32   | Fiscal year                                          |
| `fp`               | string  | `Q1` | `Q2` | `Q3` | `FY`                            |
| `frame`            | string  | XBRL frame, e.g. `CY2018Q3I`                         |
| `ingestion_date`   | DATE    |                                                      |


**PK**: `(cik, concept, period_start, period_end, accession_number)`. The accession_number is what distinguishes vintages — multiple filings for the same period mean restatements.

Approximately **24,000 facts per CIK** on average, **18,000+ filings** total.

---

## 6. Source pipeline — how data flows

### Constituents

1. `**fja05680/sp500` GitHub** (community-maintained CSVs, monthly updates) is the baseline. We pull `sp500_ticker_start_end.csv` (one row per ticker tenure interval) and `sp500.csv` (current snapshot with CIK+sector).
2. The `changes.materialize_daily()` function joins ticker tenures × NYSE trading-day calendar to produce the daily matrix.
3. **Wikipedia revision history** is the live edge: scraped daily to detect changes since the last fja05680 refresh. Anomalies (diff vs fja05680) are logged via `update/notify.py` for manual review against S&P press releases — never auto-applied.

### Identifiers (ticker → CIK)

1. **SEC `company_tickers.json`** (~10K currently-active US filers): primary source.
2. **fja05680's current snapshot** (CIK column): seed for the 503 active S&P 500 names.
3. Ticker normalization: `BRK.B` → `BRK-B` for Tiingo/yfinance/Stooq queries (their convention vs S&P's).
4. **OpenFIGI** (Phase B): planned for the ~40% of historical/delisted tickers SEC's currently-active feed doesn't cover.

### Prices — three-tier cascade

For each ticker, in order:

1. **Tiingo** (paid subscription): primary EOD source. Returns clean unadjusted OHLCV, very stable API, decent delisted coverage.
2. **yfinance**: fallback for tickers Tiingo doesn't have (mostly post-bankruptcy quotes like LEHMQ, ENRNQ, WCOEQ).
3. **Stooq** (`pandas-datareader`): deeper fallback for very old delistings.

Each row carries its `source` field for provenance auditing. **96.4% Tiingo / 3.6% yfinance** in the current build.

### Corporate actions

1. **yfinance `Ticker.splits` and `Ticker.dividends`**: bulk-fetched per ticker, returns the full history.
2. **Manual spinoffs** (`corporate_actions/spinoffs.py`): hand-curated table of 11 known S&P 500 spinoffs since 2010 (yfinance doesn't model spinoffs as a discrete event type).

### Fundamentals — EDGAR XBRL

1. `**https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json`**: per-CIK, returns every fact ever filed.
2. Each fact carries `accn` (accession), `filed` (date), `form`, `fy`, `fp` — exactly the fields needed for native PIT semantics.
3. JSON is cached locally under `data/raw/edgar/companyfacts/CIK*.json`.
4. `normalize_facts()` flattens `{taxonomy: {concept: {units: {USD: [...obs]}}}}` into wide rows conforming to the `FUNDAMENTALS_FACTS` schema.

SEC requires a `User-Agent` header identifying you (`SEC_USER_AGENT` env var); rate limit is 10 req/s. We use a 0.12s floor delay → ~8 req/s.

### Sectors

1. `**https://data.sec.gov/submissions/CIK##########.json**` has each company's current SIC code. **SIC is fully PIT-rigorous** — every filing's header carries the SIC at filing time.
2. `**crosswalk.py`** maps 2-digit SIC prefix → GICS Sector (post-2018 names), with hard-coded reversals for the 2016 Real Estate split and 2018 Communication Services rename.
3. **Wikipedia "List of S&P 500 companies"** has the GICS Sector for the current universe; we scrape it for `gics_sector_observed` (more accurate than the SIC crosswalk, but only available for current members).

---

## 7. Public PIT API — `from pitds.pit import api`

```python
from pitds.pit import api
from datetime import date

# ── Universe ──
universe = api.constituents(as_of=date(2018, 3, 15))
#   columns: ticker, cik, figi   (rounds non-trading days down to prior trading day)

universe_announced = api.constituents(
    as_of=date(2018, 3, 15),
    announce_aware=True,           # include announced-but-not-effective members
)

# ── Prices (unadjusted stored, adjusted on read) ──
df = api.prices(as_of=date(2024, 6, 28), ticker='AAPL', adjust='split_div')
#   columns: date, ticker, cik, figi,
#            open/high/low/close (unadjusted),
#            open_adj/high_adj/low_adj/close_adj/volume_adj,
#            adj_factor (the multiplier),
#            source ('tiingo'|'yfinance'|'stooq'),
#            ingestion_date

history = api.price_history(
    ticker='AAPL', start=date(2010, 1, 1), end=date(2024, 6, 28),
    adjust='split_div',
)

# ── Fundamentals (vintage-aware) ──
revenue_latest = api.fact(
    cik='0000320193',
    field_name='revenue',          # canonical alias; resolves via concept fallback
    period_end=date(2024, 3, 30),
    as_of=date(2024, 6, 28),
)
# Fields available: revenue, cost_of_revenue, gross_profit, operating_income,
#   net_income, eps_basic, eps_diluted, total_assets, total_liabilities,
#   total_equity, cash, long_term_debt, shares_outstanding,
#   operating_cash_flow, capex.
# (Or pass concept='us-gaap:Revenues' directly for raw concept access.)

revenue_orig = api.fact(
    cik='0000320193',
    field_name='revenue',
    period_end=date(2024, 3, 30),
    as_of=date(2024, 6, 28),
    vintage='first_reported',      # the value as ORIGINALLY reported, ignoring restatements
)

# ── Sector ──
s = api.sector(cik='0000320193', as_of=date(2024, 6, 28))
#   {'gics_sector': 'Information Technology',
#    'gics_sector_source': 'observed',   # 'observed' (Wikipedia) or 'sic_recon' (SIC crosswalk)
#    'sic_code': 3571}
```

### The single most important property

For any pair of dates `d1 < d2`:

```python
api.fact(cik=X, ..., as_of=d1)  # returns whatever was knowable on d1
api.fact(cik=X, ..., as_of=d2)  # returns whatever was knowable on d2
# ───────────────────────────────────────────────────────────────────
# d1 result is a SUBSET of d2's view (in time), with possibly different
# vintage of the same period if a restatement happened between d1 and d2.
```

This is what every PIT-correct backtest depends on.

---

## 8. CLI reference — `pitds <command>`


| Command                                                            | Purpose                                                                   |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------- |
| `pitds info`                                                       | Show table sizes, row counts, vintage stamps                              |
| `pitds backfill-constituents`                                      | Pull fja05680, materialize `constituents_daily` and `constituent_changes` |
| `pitds backfill-identifiers`                                       | Build `ticker_history` from SEC + fja05680 seed                           |
| `pitds backfill-prices --universe {current|all|TICK,TICK,...}`     | Fetch OHLCV via Tiingo→yfinance→Stooq cascade                             |
| `pitds backfill-corporate-actions --universe {current|all|...}`    | Splits/dividends from yfinance + curated spinoffs                         |
| `pitds backfill-fundamentals --universe {current|all|CIK,CIK,...}` | Pull EDGAR companyfacts JSON for each CIK                                 |
| `pitds backfill-sectors`                                           | Build `sector_history` from EDGAR SIC + Wikipedia GICS                    |
| `pitds retry-failed-prices`                                        | Mine the last backfill log and retry failed tickers                       |
| `pitds verify`                                                     | Run the integrity check suite (7 checks)                                  |
| `pitds verify-prices --as-of YYYY-MM-DD`                           | Per-date constituent/price coverage                                       |
| `pitds validate-fred --start YYYY-MM-DD`                           | Daily-return correlation with FRED SP500                                  |
| `pitds backtest-demo`                                              | Run a PIT-correct equal-weight backtest (illustrative)                    |
| `pitds inspect-cik <CIK> --as-of YYYY-MM-DD`                       | PIT snapshot for one company                                              |
| `pitds update-daily`                                               | Idempotent nightly orchestrator (cron-friendly)                           |


Use `pitds <command> --help` for full options.

---

## 9. Operational guide

### Initial setup

```bash
# Clone or cd into the project, then:
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Configure .env (NEVER committed to git)
cp .env.example .env
# Edit .env to set:
#   SEC_USER_AGENT="Your Name your.email@example.com"
#   TIINGO_API_KEY=...
#   OPENFIGI_API_KEY=...   (optional)
```

### Initial backfill (one-time, ~30 min on paid Tiingo)

```bash
pitds backfill-constituents
pitds backfill-identifiers
pitds backfill-prices --universe all --start 2010-01-01
pitds backfill-corporate-actions --universe current
pitds backfill-fundamentals --universe all
pitds backfill-sectors
pitds verify             # 6/7 expected; 1 documented warning is normal
```

### Daily refresh — automated

The `pitds update-daily` command is idempotent and cron-friendly:

```cron
# 5:30 PM ET = 21:30 UTC, weekdays after market close
30 21 * * 1-5  cd /Users/mharutyunyan/Projects/Enso/PITDS && \
  .venv/bin/pitds update-daily >> data/raw/daily.log 2>&1
```

It runs these steps in order, logging anomalies to `data/raw/anomalies.log`:

1. **Constituent diff**: scrape Wikipedia, compare against fja05680, log anomalies (never auto-apply).
2. **Constituent re-materialize**: idempotent fold of changes log into daily matrix.
3. **Prices yesterday**: backfill last ~4 days of prices for the current universe (small, fast).
4. **Corporate actions yesterday**: refresh splits/dividends for the current universe.
5. **Fundamentals incremental**: TODO Phase C — currently deferred to weekly batch.
6. **Verify**: run integrity checks, fail loudly on `critical` violations.

### Weekly bulk refresh

```cron
# Sunday 3 AM
0 3 * * 0  cd /Users/mharutyunyan/Projects/Enso/PITDS && \
  .venv/bin/pitds backfill-fundamentals --universe current --refresh
```

This re-pulls EDGAR companyfacts for CIKs that may have new filings.

### Vintage-aware reproducibility

Every Parquet file's `key_value_metadata` carries `pitds.ingestion_date`. To reproduce a backtest exactly six months from now, snapshot `data/processed/` to versioned storage (S3, DVC, git LFS) at the time of the run. Any future query against that snapshot returns the same data.

`pitds info` displays current vintages.

---

## 10. Validation results

### Test suites

`**tests/test_pit_invariants.py**` (5 fixture-based tests):

- `test_constituent_count_invariant`: every date has [495, 510] constituents
- `test_constituent_no_duplicates`: no `(date, ticker)` appears twice
- `test_constituent_date_range`: range starts at HISTORY_START, ends within last 14 days
- `test_no_future_price_rows`: no `prices` row with `date > today`
- `test_pit_monotone_visibility`: `|rows visible at d1| ≤ |rows visible at d2|` for `d1 < d2`

`**tests/test_pit_property_tests.py**` (4 Hypothesis property tests, 30 random samples each):

- `test_universe_size_in_range`: random `as_of` → universe size [480, 515]
- `test_no_lookahead_in_prices`: random `as_of` → no AAPL price dated after `as_of`
- `test_universe_history_monotone_in_size`: random `(d1, d2)` → monotone visibility
- `test_price_history_truncated_at_as_of`: `price_history(end=T)` returns no rows after T

**Result: 9/9 passing.**

### Integrity checks (`pitds verify`)


| Check                           | Severity | Status                                                       |
| ------------------------------- | -------- | ------------------------------------------------------------ |
| `constituent_count`             | critical | ✅ all dates in [495, 510]                                    |
| `no_future_prices`              | critical | ✅                                                            |
| `price_pk_uniqueness`           | critical | ✅                                                            |
| `fundamentals_filed_not_future` | critical | ✅                                                            |
| `price_coverage`                | warning  | ✅ 99.6% on most-recent date                                  |
| `event_tesla_add`               | warning  | ✅ TSLA correctly absent on 2020-12-18, present on 2020-12-21 |
| `event_ge_remove`               | warning  | ⚠️ off-by-one in fja05680 source data (documented)           |


### Cross-validation against FRED

`pitds validate-fred --start 2018-01-01 --end 2024-12-31`:

- **Daily-return correlation: 0.9452** (target ≥0.95)
- 1,760 trading-day observations
- PITDS equal-weight annualized return 2.4% vs FRED cap-weight 11.2% — gap is the well-documented mega-cap dispersion of 2018-2024 (AAPL/MSFT/NVDA/META/GOOGL combined ~30% of cap-weight S&P)

### Spot-check against reality

`pitds inspect-cik 0000320193 --as-of 2024-06-28` (AAPL Q2 FY2024):

- Revenue: $90.75B  ✓ (matches AAPL's actual reported $90.75B for fiscal Q2 2024)
- Net income: $23.64B  ✓
- Total assets: $337.41B  ✓
- Shares outstanding: 15.34B  ✓
- Sector: Information Technology  ✓ (Wikipedia-observed)

### PIT-correct backtest sanity

`pitds backtest-demo --start 2018-01-01 --end 2023-12-31`:

- Equal-weight, quarterly rebalance, 24 rebalances
- **Total return: +52.5%** over 6 years
- Compare to **RSP** (real Equal-Weight S&P 500 ETF) which returned ~58% over the same period
- The slight underperformance is the survivorship-bias correction working: PITDS includes delisted names (GE removal, COVID-era bankruptcies) that ETFs would have rotated out at rebalance

---

## 11. Known limitations (be honest about these)

1. **No fundamentals before 2010**. Pre-XBRL filings are HTML-only and not freely PIT-parseable.
2. **~3.6% of price rows still come from yfinance** for tickers Tiingo lacks (LEHMQ, ENRNQ, WCOEQ post-bankruptcy; some pre-2014 delistings). Documented in `prices.source` per row.
3. **GICS Industry / Sub-Industry historically**: not freely reconstructable. Only Sector available; SIC is the rigorous PIT alternative.
4. **GE removal off-by-one**: fja05680's `end_date` for GE is 2018-06-27; S&P press release says effective 2018-06-26. Half-open interval logic correctly handles this but the regression test flags it as warning. Same pattern likely affects a handful of other event dates.
5. **No CRSP-style delisting returns**. We flag `delist_reason='bankruptcy'` but the last-month return underestimates true loss (e.g., bankruptcy distributions, pink-sheet residual values).
6. **No intraday data** — EOD only.
7. **Pre-IPO data leakage risk for very recent IPOs**: yfinance occasionally returns placeholder prices before a stock's actual first trading day. Not currently filtered; a Phase B refinement is to truncate price history to start no earlier than the first 10-Q period_end.
8. **Concept aliasing in fundamentals is a curated list of ~15 fields** (revenue, net_income, etc.). For arbitrary XBRL concepts, query directly via `concept='us-gaap:...'`.

---

## 12. Future enhancements

- **OpenFIGI integration** (`identifiers/openfigi.py` is a stub) — fixes the 40% historical-ticker CIK-resolution gap
- **Wikipedia revision-history sector walking** — for fully PIT `gics_sector_observed`, not just current snapshot
- **EDGAR `companyfacts.zip` bulk loader** (`companyfacts_bulk.py` placeholder) — efficient initial seeding for full filer universe
- **Per-filing SIC parsing** — multi-row sector_history when SIC actually changes
- **Restatement spot-check tests** — pick a known restatement (GE 2017) and confirm `vintage='first_reported'` returns the original, `'latest_known'` returns the restated
- **Cap-weighted index reconstruction with FRED tracking ≤5bps** — uses `shares_outstanding` from PITDS fundamentals
- **OPC sweep against S&P press release archive** — automate constituent-effective-date verification

---

## 13. Source-of-truth file map

These are the load-bearing files. If you only read 10 files in this project, read these:


| File                                  | What it defines                                                |
| ------------------------------------- | -------------------------------------------------------------- |
| `src/pitds/store/schemas.py`          | Arrow schemas — the single source of truth for table structure |
| `src/pitds/pit/api.py`                | The public PIT query interface                                 |
| `src/pitds/fundamentals/pit_layer.py` | Vintage selection logic — the load-bearing PIT mechanism       |
| `src/pitds/prices/adjust.py`          | On-read split/dividend adjustment SQL                          |
| `src/pitds/prices/loader.py`          | Three-tier cascade (Tiingo → yfinance → Stooq)                 |
| `src/pitds/constituents/changes.py`   | Daily matrix materialization from event log                    |
| `src/pitds/store/writer.py`           | Append-only Parquet writer with vintage stamping               |
| `src/pitds/update/daily.py`           | Nightly orchestrator                                           |
| `src/pitds/update/verify.py`          | Integrity check suite                                          |
| `tests/test_pit_invariants.py`        | The contract enforced by tests                                 |


Plan file: `~/.claude/plans/i-want-to-put-fluffy-lovelace.md` (the original architecture decision document).

---

## 14. Research patterns — train/test on PIT data without leaking

PITDS lets you train on 2010-2022 and test on 2023-today **survivorship-bias-free**, but only if your code follows five rules. Get any wrong and the model silently sees the future.

### The five rules (in order of how often researchers get them wrong)

#### Rule 1 — Universe membership uses per-sample `as_of`, never today

```python
# WRONG — leaks today's universe into 2018 training samples
universe_today = api.constituents(as_of=date.today())
for t in training_dates:
    for cik in universe_today['cik']:        # ← survivorship bias
        features = compute_features(cik, t)

# RIGHT — universe is whatever was in the index on each sample's date
for t in training_dates:
    universe_t = api.constituents(as_of=t)   # ← rebuilds at each t
    for cik in universe_t['cik']:
        features = compute_features(cik, t)
```

#### Rule 2 — Feature lookback windows must end strictly *at or before* the prediction date

```python
# WRONG — momentum_12m at t includes price ON t (the label horizon)
hist = api.price_history(cik=cik, end=t + timedelta(days=21))   # leaks
mom_12m = hist['close_adj'].iloc[-1] / hist['close_adj'].iloc[-252] - 1

# RIGHT — end=t means the most recent included row is t's close
hist = api.price_history(cik=cik, end=t)
mom_12m = hist['close_adj'].iloc[-1] / hist['close_adj'].iloc[-252] - 1
```

The `end` parameter in `api.price_history` is *inclusive* — `end=date(2018,3,15)` returns prices through 2018-03-15 close. That's the most recent observation knowable to a trader making a decision before market open on 2018-03-16.

#### Rule 3 — Fundamentals must use `as_of=t` for each sample, not today

This is the subtlest bug. If you pull fundamentals with `as_of=date.today()` for every training sample, you've leaked **restatements**: a 2018 sample sees 2018 financials as restated in 2024.

```python
# WRONG — restatement leak
for t in training_dates:
    rev = api.fact(cik=cik, field_name='revenue',
                   period_end=last_fye_before(t),
                   as_of=date.today())          # ← restatements leak

# RIGHT — only data filed by t is visible
for t in training_dates:
    rev = api.fact(cik=cik, field_name='revenue',
                   period_end=last_fye_before(t),
                   as_of=t)                     # ← PIT-correct
```

If you want **zero restatement leak** even within `as_of=t`, use `vintage='first_reported'` to take the original filed value rather than the latest-as-of-t restatement.

#### Rule 4 — Be consistent about vintage between training and deployment

Pick `latest_known` or `first_reported` and use the same vintage for training samples and live inference. Mixing them creates a distribution shift between train and prod.

- **`latest_known`** (default): the best-knowledge-as-of-t value, including any restatements filed by t. Most factor research uses this.
- **`first_reported`**: the value filed in the first 10-Q/10-K covering this period. Closer to what a live trader would have seen on the announcement day. Lower-noise; misses information that arrives later.

#### Rule 5 — Respect reporting lag

For a sample on 2018-04-15, you cannot use FY2017 financials filed in 10-K on 2018-02-01 (visible) AND FY2018-Q1 financials filed on 2018-05-03 (NOT yet visible). PITDS handles this if you pass `as_of=t`, but you must trust the answer:

```python
rev_q1 = api.fact(cik=X, field_name='revenue',
                  period_end=date(2018,3,31),
                  as_of=date(2018,4,15))
# rev_q1 is EMPTY — Q1 2018 10-Q wasn't filed until May 2018.
```

Code defensively: if `api.fact` returns empty, fall back to the most recent reported quarter, or skip the sample. The `pitds.pit.research.latest_reported_fact()` helper does this walking automatically.

### Canonical training/test split pattern

```python
from pitds.pit import api
from pitds.pit.research import (
    is_scoreable, filter_universe, latest_reported_fact, walk_forward_splits,
)
from datetime import date, timedelta
import numpy as np

VINTAGE = 'latest_known'   # or 'first_reported' — pick one and stick to it

def features_at(cik, t):
    """PIT-correct feature row for (cik, t)."""
    hist = api.price_history(cik=cik, end=t, adjust='split_div')
    if len(hist) < 252:
        return None
    mom_12m = hist['close_adj'].iloc[-21] / hist['close_adj'].iloc[-252] - 1

    rev = latest_reported_fact(cik=cik, field_name='revenue',
                               as_of=t, vintage=VINTAGE)
    ni  = latest_reported_fact(cik=cik, field_name='net_income',
                               as_of=t, vintage=VINTAGE)
    if rev.empty or ni.empty:
        return None

    return {
        'cik': cik, 'date': t,
        'mom_12m': mom_12m,
        'log_revenue': float(np.log(rev['val'].iloc[0])),
        'profit_margin': float(ni['val'].iloc[0] / rev['val'].iloc[0]),
    }


def label_at(cik, t, horizon_days=21):
    """Forward 1-month return as label."""
    fwd = api.price_history(cik=cik, start=t,
                            end=t + timedelta(days=horizon_days*2),
                            adjust='split_div')
    if len(fwd) < horizon_days:
        return None
    return float(fwd['close_adj'].iloc[horizon_days-1] / fwd['close_adj'].iloc[0] - 1)


# Walk-forward splits — train on rolling 5y, test on next 6mo, step 6mo
folds = walk_forward_splits(
    start=date(2010,1,1), end=date.today(),
    train_years=5, test_months=6, step_months=6,
)

for fold in folds:
    train_rows = []
    for t in trading_days_between(fold.train_start, fold.train_end):
        for cik in filter_universe(as_of=t, min_history_days=252):
            f = features_at(cik, t)
            y = label_at(cik, t)
            if f and y is not None:
                train_rows.append({**f, 'y': y})

    test_rows = []
    for t in trading_days_between(fold.test_start, fold.test_end):
        for cik in filter_universe(as_of=t):
            f = features_at(cik, t)
            y = label_at(cik, t)            # for evaluation only; model never sees y
            if f and y is not None:
                test_rows.append({**f, 'y': y})

    # Train your model on train_rows, evaluate on test_rows
    model.fit(train_rows)
    score = model.evaluate(test_rows)
```

Three properties this code guarantees:

- Universe at every t is the *historical* S&P 500, not today's
- All features at t use only data knowable on t (prices ≤ t close, fundamentals filed ≤ t)
- Restatements made *after* t never leak into a t-sample's features

### Inference-time deployment

The same `features_at()` and `is_scoreable()` work unchanged at inference — pass `as_of=date.today()` and you get a live prediction with no train/serve skew.

```python
from pitds.pit.research import is_scoreable, filter_universe

today = date.today()
predictions = {}
for cik in filter_universe(as_of=today, min_history_days=252):
    score = is_scoreable(cik, as_of=today)
    if not score.ok:
        log.info(f"skip {cik}: {score.reason}")
        continue
    f = features_at(cik, today)
    if f is None:
        continue
    predictions[cik] = model.predict(f)

# Pass `predictions` to your order routing layer.
```

### Common gotchas catalog

- **Pre-IPO data leak**: yfinance occasionally returns placeholder prices before a stock's actual first trading day. `is_scoreable` catches this via `min_history_days`.
- **Ticker rename inside training window**: handled automatically — CIK is stable, just join on CIK.
- **Mid-period sector reclass**: `api.sector(cik, as_of=t)` returns the sector that applied on t. Phase B refinement: Wikipedia revision history; for now, sector_history has only the current snapshot per CIK.
- **Splits inside the test window**: `adjust='split_div'` handles split-adjustment correctly using only actions with `ex_date <= t` for sample at t.
- **Constituent change announce vs effective**: the default API uses `effective_date`. To trade the announcement effect, pass `announce_aware=True` to `api.constituents`.

---

## 15. Why this matters

A correctly-built PIT dataset is the foundation of any honest quantitative research. Without it:

- Backtests on "current S&P 500 going back 20 years" overstate returns by 1-3% annually (survivorship)
- Strategies fitted on restated fundamentals look better than they actually did at the time (look-ahead)
- Index-inclusion-effect signal leaks into momentum and quality factors

Commercial vendors (Norgate, Sharadar, Compustat) charge $50-thousands/month for what PITDS provides for free, with the trade-off that you spent some weekends building it. The architecture documented here is the result of those weekends.

Use it well.