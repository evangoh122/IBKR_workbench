# yfinance Staging Feed + Polygon Daily Flat File — Design

**Date:** 2026-06-06
**Branch:** `claude`
**Status:** Draft — pending user approval

---

## 0. Hard constraints for the implementer (read before writing any code)

- **DO NOT modify, replace, or refactor `etl/bulk_load_massive.py`.** It loads Polygon **minute** bars and must continue to work unchanged. The new daily loader (`etl/bulk_load_daily.py`) is **additive — a parallel sibling, not a replacement**.
- **DO NOT modify or replace `etl/extract_polygon_ticks.py` or the `polygon-ticks` / `polygon-semis` jobs.** Tick extraction stays as-is.
- **DO NOT remove or rename the existing `timespan='minute'` rows in `polygon_bars`.** The new daily flat-file loader writes `timespan='day'` rows alongside them.
- If the implementer feels something in `bulk_load_massive.py` would be cleaner as a shared helper — resist. Out of scope for this spec.

---

## 1. What we're trying to accomplish

Three concrete deliverables, one underlying goal: **broaden the project's price-data coverage and give us a way to trust the data we already have.**

| # | Deliverable | One-line purpose |
|---|---|---|
| 1 | yfinance daily bars for the 32 semi/Mag-7 tickers | Independent second source to cross-check the Polygon minute bars already loaded by `bulk_load_massive.py` |
| 2 | yfinance daily bars + statistics for 18 major global indices | Macro context — let the dashboard and chat engine answer "how is the broad market behaving?" with proper history |
| 3 | Polygon **daily** flat-file loader (`day_aggs_v1`) for the same 32 tickers | Fast bulk path for daily history that mirrors the existing minute-bar bulk path; cheaper and faster than the per-ticker REST endpoint already in `extract_polygon.py` |

Push design + plan to the `claude` branch so a separate mimo (opencode) session can pick them up and implement; a Sonnet PR review closes the loop.

---

## 2. Why each piece exists

### 2.1 yfinance validation feed against the semis

`etl/bulk_load_massive.py` has loaded minute bars for 32 tickers (Mag 7 + semiconductors) into `polygon_bars`. There is currently **no independent way to verify** that those bars are correct. yfinance gives us free, easy daily OHLC back to listing date for those same tickers — landing it side-by-side as `staging_yf_bars` lets us write a single comparison query (yfinance close vs polygon daily close per ticker per day) and flag divergences. This is also the foundation for any future "data quality" page on the dashboard.

**Why `staging_` prefix and not a separate file:** keeps the comparison query trivial (one DB, no `ATTACH`), but the name makes it obvious this isn't production-grade data and shouldn't be served from the dashboard's main pages.

### 2.2 yfinance major indices + statistical measures

The project currently has no broad-market index history. To answer questions like "how did the Russell react when the S&P moved 3σ?" we need long history for the major US + global benchmarks. yfinance is the right source: free, deep history, easy ETF + index coverage.

The **18 indices** the user explicitly named, with sensible yfinance symbols:

| Name | Symbol |
|---|---|
| MSCI All Country | `ACWI` |
| MSCI All Country ex-US | `ACWX` |
| S&P 500 | `^GSPC` |
| S&P "Rest of the World" (Developed ex-US) | `SPDW` (SPDR Portfolio Developed World ex-US — S&P-branded, yfinance-accessible) |
| S&P 500 Equal Weight | `RSP` |
| Dow Jones Industrial Average | `^DJI` |
| Nasdaq Composite | `^IXIC` |
| S&P 1500 Total Market | `SPTM` |
| S&P MidCap 400 | `MDY` |
| S&P SmallCap 600 | `^SP600` |
| Russell 2000 | `^RUT` |
| SMH (Semiconductor ETF) | `SMH` |
| IGV (Software ETF) | `IGV` |
| MSCI Europe | `EZU` |
| MSCI Emerging Markets | `EEM` |
| MSCI Japan | `EWJ` |
| Nasdaq − S&P 500 (derived) | `^IXIC_MINUS_GSPC` (computed from close prices) |
| Russell 2000 − S&P 500 (derived) | `^RUT_MINUS_GSPC` (computed from close prices) |

Total: **16 real yfinance symbols + 2 derived spread symbols = 18 entries** in `staging_yf_indices`. The two `_MINUS_` rows are computed from their constituents at load time, written to `staging_yf_indices` as if they were a regular symbol, so the stats pipeline treats them uniformly.

**Statistical measures stored per `(symbol, ts)` in `staging_yf_index_stats`:**

| Column | Definition |
|---|---|
| `ret_1d`, `ret_21d`, `ret_252d` | Log returns over 1 / 21 / 252 trading days |
| `vol_21d`, `vol_63d`, `vol_252d` | Annualised realised volatility (stdev of log returns × √252) over each window |
| `drawdown` | Current drawdown from rolling 252d high |
| `max_drawdown_to_date` | Worst peak-to-trough decline over all history up to `ts` |
| `sharpe_252d` | Annualised mean log return / annualised vol over trailing 252d (risk-free assumed 0 — note in code) |
| `skew_252d`, `kurt_252d` | Trailing 252d sample skew + excess kurtosis of daily log returns |
| `mean_252d`, `sigma_252d` | Trailing 252d mean and stdev of daily log returns |
| `band_plus_1`, `band_minus_1`, `band_plus_2`, `band_minus_2`, `band_plus_3`, `band_minus_3`, `band_plus_4`, `band_minus_4` | `mean_252d ± kσ_252d` for k ∈ {1,2,3,4} — the σ-bands the user asked for |

The stats table is **rebuilt every run** (TRUNCATE + INSERT). Cheap (~18 indices × ~25 years of trading days ≈ 113k rows) and avoids drift between historical raw and historical stats.

### 2.3 Polygon daily flat-file loader

`extract_polygon.py` already pulls daily bars via the per-ticker REST API. That's fine for small lookbacks but slow at scale and burns API quota. Polygon also ships a daily aggregate flat file (`s3://flatfiles/us_stocks_sip/day_aggs_v1/`) — one CSV.gz per trading day, ~7 MB each, containing every US ticker. We already use the equivalent `minute_aggs_v1` path in `bulk_load_massive.py`.

The new `etl/bulk_load_daily.py` is **a faithful mirror** of `bulk_load_massive.py`:
- Same 32-ticker filter (Mag 7 + semis — held in a module-level `TICKERS` set, easy to edit).
- Same AWS CLI sync.
- Same DuckDB `read_csv` ingestion path.
- Only differences: bucket path, download dir (`data/day_aggs/`), `timespan='day'` on insert.

We **do not refactor** `bulk_load_massive.py` to share code with the new module. Keeping the two parallel and obviously-related is simpler than introducing a shared helper that would force a touch on already-tested code. If a third flat-file loader appears later, that's the right moment to extract a helper.

---

## 3. Architecture

```
yfinance ─► etl/extract_yfinance.py ─► staging_yf_bars         (32 tickers, daily, full history)
                                    ─► staging_yf_indices      (18 indices, daily, max history)
                                    ─► staging_yf_index_stats  (derived per (symbol, ts))

Polygon S3 ──► etl/bulk_load_daily.py ─► polygon_bars (timespan='day')   (32 tickers, day_aggs_v1 flat files)
```

All three streams land in `ibkr.duckdb`. Three new `--job` entries in `main.py`:

```
python main.py --job yf-bars             # 32-ticker validation feed
python main.py --job yf-indices          # 18 indices + stats recompute
python main.py --job polygon-daily-flat  # daily flat-file bulk load
```

---

## 4. Components

### `etl/extract_yfinance.py` (new)

| Symbol | Purpose |
|---|---|
| `TICKERS` (module-level set) | Same 32 tickers as `bulk_load_massive.py` — imported from there, not re-listed, to keep them in sync |
| `INDICES` (module-level list of dicts) | The 17 yfinance symbols above plus the 2 derived spread symbols |
| `_fetch_one(symbol, period="max", interval="1d")` | Single-symbol yfinance call with one retry on transient errors |
| `_insert_bars(conn, table, symbol_col, rows)` | Shared insert path for bars and indices |
| `_compute_derived_spreads(conn)` | Builds `^IXIC_MINUS_GSPC` and `^RUT_MINUS_GSPC` rows in `staging_yf_indices` from close prices |
| `_compute_index_stats(conn)` | Pure-SQL rebuild of `staging_yf_index_stats` using DuckDB window functions |
| `run_yf_bars_etl()` | Top-level entry — returns row count |
| `run_yf_indices_etl()` | Top-level entry — bars + spreads + stats; returns row count |

### `etl/bulk_load_daily.py` (new — sibling of `bulk_load_massive.py`)

| Symbol | Purpose |
|---|---|
| `S3_BUCKET = "s3://flatfiles/us_stocks_sip/day_aggs_v1"` | Daily aggregates path |
| `DOWNLOAD_DIR = Path("data/day_aggs")` | Local landing zone |
| `TICKERS` | Imported from `etl.bulk_load_massive` to stay in sync |
| `download_year(year)` / `load_file(path, conn)` / `main()` | Same shapes as `bulk_load_massive.py`; insert uses `timespan='day'` |

### `db/database.py:init_db()` — three new CREATE TABLE blocks

```sql
CREATE TABLE IF NOT EXISTS staging_yf_bars (
    ticker      TEXT NOT NULL,
    ts          TEXT NOT NULL,              -- ISO date (yfinance daily bars are date-only)
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    adj_close   REAL,
    volume      REAL,
    dividends   REAL,
    splits      REAL,
    created_at  TIMESTAMP DEFAULT now(),
    UNIQUE(ticker, ts)
);
CREATE INDEX IF NOT EXISTS idx_syfb_ticker_ts ON staging_yf_bars(ticker, ts);

CREATE TABLE IF NOT EXISTS staging_yf_indices (
    symbol      TEXT NOT NULL,
    ts          TEXT NOT NULL,
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    adj_close   REAL,
    volume      REAL,
    created_at  TIMESTAMP DEFAULT now(),
    UNIQUE(symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_syfi_symbol_ts ON staging_yf_indices(symbol, ts);

CREATE TABLE IF NOT EXISTS staging_yf_index_stats (
    symbol               TEXT NOT NULL,
    ts                   TEXT NOT NULL,
    ret_1d               REAL,
    ret_21d              REAL,
    ret_252d             REAL,
    vol_21d              REAL,
    vol_63d              REAL,
    vol_252d             REAL,
    drawdown             REAL,
    max_drawdown_to_date REAL,
    sharpe_252d          REAL,
    skew_252d            REAL,
    kurt_252d            REAL,
    mean_252d            REAL,
    sigma_252d           REAL,
    band_plus_1          REAL,
    band_minus_1         REAL,
    band_plus_2          REAL,
    band_minus_2         REAL,
    band_plus_3          REAL,
    band_minus_3         REAL,
    band_plus_4          REAL,
    band_minus_4         REAL,
    created_at           TIMESTAMP DEFAULT now(),
    UNIQUE(symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_syfis_symbol_ts ON staging_yf_index_stats(symbol, ts);
```

### `main.py` — three new jobs

- `@etl_job("yf-bars")` `job_yf_bars()` → `run_yf_bars_etl()`
- `@etl_job("yf-indices")` `job_yf_indices()` → `run_yf_indices_etl()`
- `@etl_job("polygon-daily-flat")` `job_polygon_daily_flat()` → calls `etl.bulk_load_daily.run()` programmatically. Year range: read from `START_DATE` / `END_DATE` env vars (parse the year out); default `2021–2026` matching `bulk_load_massive.py`. Returns row count from the loader.

Choices list in `argparse` gets `"yf-bars"`, `"yf-indices"`, `"polygon-daily-flat"`.

### `requirements.txt`

Add `yfinance>=0.2.40,<1`.

### `README.md`

Append three rows under **ETL Jobs** documenting the new commands, and one row in the Database Schema table per new staging table.

---

## 5. Error handling

| Failure | Behaviour |
|---|---|
| yfinance returns empty df for a symbol (rate-limit / delisted) | `logger.warning`, skip symbol, continue run |
| yfinance raises `requests.exceptions.*` | One retry with 5s backoff per symbol; on second failure, log and skip |
| Stats computation has insufficient history (< 252d) | Long-window columns return `NULL`; row still inserted |
| Bulk daily flat file: malformed row | Same `try/except` + `logger.warning` per row as `bulk_load_massive.py` |
| Job-level failure | `@etl_job` decorator already writes `error` status + message to `etl_runs` |

All inserts use `INSERT OR IGNORE` on the UNIQUE keys → re-runs are idempotent.

---

## 6. Testing

| Test file | What it asserts |
|---|---|
| `tests/bronze/test_bronze_yfinance_bars.py` | Patched `yf.Ticker(...).history()` returns a fixture df → correct rows in `staging_yf_bars`, idempotent on re-run, empty df handled |
| `tests/bronze/test_bronze_yfinance_indices.py` | Same for `staging_yf_indices`, plus the two derived `_MINUS_` rows are present with `close = close_a − close_b` |
| `tests/silver/test_silver_index_stats.py` | Synthetic 300-day price series → closed-form expected values for returns, vol, drawdown, σ-bands; rows with < 252 days history have NULL long-window columns |
| `tests/bronze/test_bronze_bulk_load_daily.py` | Synthetic `day_aggs_v1` `.csv.gz` → only the 32 tickers land, `timespan='day'`, idempotent re-load |

Run via existing `pytest` config (`pytest.ini` already present).

---

## 7. Out of scope

- **Touching `etl/bulk_load_massive.py` (minute loader) in any way — keep it untouched.**
- **Touching `etl/extract_polygon_ticks.py` or the `polygon-ticks` / `polygon-semis` jobs — keep them untouched.**
- Refactoring `bulk_load_massive.py` to share code with `bulk_load_daily.py` — defer until a third flat-file loader appears.
- Dashboard pages for the new staging tables — separate task; this design only lands the data.
- yfinance for the full ~11k ticker universe — explicitly rejected; would be slow and risks IP bans for marginal value.
- Adjusted-vs-unadjusted reconciliation between yfinance (adjusted by default) and Polygon (split/dividend-aware but reported unadjusted) — flagged here as a known wrinkle the comparison query will need to handle, but not solved in this spec.

---

## 8. Hand-off

When the spec + implementation plan are committed and pushed to the `claude` branch, a separate mimo (opencode) session will read both and write the code. A Sonnet code review on the resulting PR closes the loop.
