# yfinance Staging Feed + Polygon Daily Flat File — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add yfinance daily bars for 32 validation tickers + 18 indices with statistics, and a Polygon daily-flat-file loader — all additive, all behind new `--job` entries, no changes to existing minute/tick loaders.

**Architecture:** Three new ETL streams landing in `ibkr.duckdb`. yfinance code consolidated in one module (`etl/extract_yfinance.py`) that writes `staging_yf_bars`, `staging_yf_indices`, and a recomputed-every-run `staging_yf_index_stats`. A separate `etl/bulk_load_daily.py` mirrors `etl/bulk_load_massive.py` for Polygon `day_aggs_v1` S3 flat files, writing `timespan='day'` rows to `polygon_bars`.

**Tech Stack:** Python 3.11, DuckDB (with window functions for stats), `yfinance` (new dep), `pandas` (already present), `loguru`, `pytest`. AWS CLI for S3 sync (already used by `bulk_load_massive.py`).

**Companion spec:** `docs/superpowers/specs/2026-06-06-yfinance-staging-and-polygon-daily-flat-design.md` — read Section 0 (hard constraints) before touching any code.

---

## Hard constraints (re-stated from spec Section 0)

1. **Do NOT modify, replace, or refactor `etl/bulk_load_massive.py`** (minute loader stays untouched).
2. **Do NOT modify `etl/extract_polygon_ticks.py` or the `polygon-ticks` / `polygon-semis` jobs.**
3. **Do NOT remove or rename existing `timespan='minute'` rows in `polygon_bars`.** Daily loader writes alongside them.
4. **Do NOT extract a shared flat-file helper** between `bulk_load_massive.py` and the new `bulk_load_daily.py`. Keep them parallel siblings.

---

## File map

| Action | Path | Responsibility |
|---|---|---|
| Create | `etl/extract_yfinance.py` | yfinance client wrapper, bars + indices ETL, derived spreads, stats computation |
| Create | `etl/bulk_load_daily.py` | Polygon S3 `day_aggs_v1` sync + filtered load into `polygon_bars` (timespan='day') |
| Modify | `db/database.py` | Add 3 `CREATE TABLE` blocks: `staging_yf_bars`, `staging_yf_indices`, `staging_yf_index_stats` |
| Modify | `main.py` | Add 3 `@etl_job` functions + 3 `argparse` choices: `yf-bars`, `yf-indices`, `polygon-daily-flat` |
| Modify | `requirements.txt` | Add `yfinance>=0.2.40,<1` |
| Modify | `README.md` | Add 3 ETL Jobs rows + 3 Database Schema rows |
| Create | `tests/bronze/test_bronze_yfinance_bars.py` | Bars ingestion + idempotency |
| Create | `tests/bronze/test_bronze_yfinance_indices.py` | Indices ingestion + derived spread rows |
| Create | `tests/silver/test_silver_index_stats.py` | Stats computation (returns, vol, drawdown, σ-bands) |
| Create | `tests/bronze/test_bronze_bulk_load_daily.py` | Filtered load of synthetic `day_aggs_v1` csv.gz |

---

## Task ordering & dependency rationale

1. **Task 1** lays the table schemas — every later task either inserts to or queries them.
2. **Task 2** adds the yfinance dependency before any import.
3. **Tasks 3 & 4** build `staging_yf_bars` (TDD). Simplest yfinance flow first.
4. **Tasks 5 & 6** build `staging_yf_indices` raw bars including the two derived spreads (TDD).
5. **Tasks 7 & 8** build `staging_yf_index_stats` SQL (TDD). Depends on indices being loadable.
6. **Tasks 9 & 10** build `bulk_load_daily.py` (TDD). Independent of yfinance.
7. **Task 11** wires `main.py` jobs. Depends on all module-level functions existing.
8. **Task 12** documents in README.
9. **Task 13** end-to-end smoke + branch push.

---

## Task 1: Add the three staging tables to the schema

**Files:**
- Modify: `db/database.py` — insert new `CREATE TABLE` blocks inside `init_db()`, after the existing `cot_reports` block, before the `Vector Storage` section
- Test: `tests/bronze/test_bronze_yfinance_bars.py` (new — single tiny test just to assert tables exist)

- [ ] **Step 1: Write the failing test**

Create `tests/bronze/test_bronze_yfinance_bars.py` with this content:

```python
"""
tests/bronze/test_bronze_yfinance_bars.py
Bronze layer: yfinance daily bars ingestion into staging_yf_bars.
"""
from db.database import get_connection


def test_staging_tables_exist(tmp_db):
    """init_db must create the three new staging tables."""
    with get_connection() as conn:
        tables = {
            row[0] for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
    assert "staging_yf_bars" in tables
    assert "staging_yf_indices" in tables
    assert "staging_yf_index_stats" in tables
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/bronze/test_bronze_yfinance_bars.py::test_staging_tables_exist -v`
Expected: FAIL — `assert "staging_yf_bars" in tables` fails (tables don't exist yet).

- [ ] **Step 3: Add the three CREATE TABLE blocks**

In `db/database.py`, locate the existing block that ends with:

```python
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_cot_market_date
                ON cot_reports(market_name, report_date)
        """)
```

Immediately after that block (and before the `# ── EDGAR: 13-F institutional holdings ─` block), insert:

```python
        # ── Staging: yfinance daily bars (validation feed) ────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS staging_yf_bars (
                ticker      TEXT NOT NULL,
                ts          TEXT NOT NULL,
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
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_syfb_ticker_ts
                ON staging_yf_bars(ticker, ts)
        """)

        # ── Staging: yfinance major indices ──────────────────────────────────
        conn.execute("""
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
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_syfi_symbol_ts
                ON staging_yf_indices(symbol, ts)
        """)

        # ── Staging: yfinance index statistics (recomputed every run) ────────
        conn.execute("""
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
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_syfis_symbol_ts
                ON staging_yf_index_stats(symbol, ts)
        """)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/bronze/test_bronze_yfinance_bars.py::test_staging_tables_exist -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add db/database.py tests/bronze/test_bronze_yfinance_bars.py
git commit -m "feat(db): add three yfinance staging tables to init_db"
```

---

## Task 2: Add `yfinance` to requirements

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Add the dependency**

Open `requirements.txt`. Locate the line `pandas>=2.2.0,<3` (near the bottom). Immediately above it, add:

```
yfinance>=0.2.40,<1
```

- [ ] **Step 2: Install locally so subsequent tests can import it**

Run: `pip install "yfinance>=0.2.40,<1"`
Expected: install succeeds (or "already satisfied").

- [ ] **Step 3: Verify import works**

Run: `python -c "import yfinance; print(yfinance.__version__)"`
Expected: prints a version string ≥ 0.2.40.

- [ ] **Step 4: Commit**

```bash
git add requirements.txt
git commit -m "chore: add yfinance dependency for staging feeds"
```

---

## Task 3: yfinance bars ETL — failing test

**Files:**
- Test: `tests/bronze/test_bronze_yfinance_bars.py` (append)

- [ ] **Step 1: Append the failing test**

Append to `tests/bronze/test_bronze_yfinance_bars.py`:

```python
from unittest.mock import patch, MagicMock
import pandas as pd


def _fake_history_df():
    """Build a pandas DataFrame in the exact shape yfinance returns from .history()."""
    idx = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])
    return pd.DataFrame(
        {
            "Open":      [100.0, 101.0, 102.0],
            "High":      [105.0, 106.0, 107.0],
            "Low":       [ 95.0,  96.0,  97.0],
            "Close":     [102.0, 103.0, 104.0],
            "Adj Close": [101.5, 102.5, 103.5],
            "Volume":    [1_000_000, 1_100_000, 1_200_000],
            "Dividends": [0.0, 0.0, 0.25],
            "Stock Splits": [0.0, 0.0, 0.0],
        },
        index=idx,
    )


def test_yf_bars_writes_rows(tmp_db):
    """run_yf_bars_etl(['NVDA']) writes 3 rows to staging_yf_bars."""
    from etl.extract_yfinance import run_yf_bars_etl

    fake_ticker = MagicMock()
    fake_ticker.history.return_value = _fake_history_df()

    with patch("etl.extract_yfinance.yf.Ticker", return_value=fake_ticker), \
         patch("etl.extract_yfinance._RATE_DELAY", 0):
        count = run_yf_bars_etl(tickers=["NVDA"])

    assert count == 3
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT ticker, ts, open, close, adj_close, volume, dividends "
            "FROM staging_yf_bars ORDER BY ts"
        ).fetchall()
    assert rows[0] == ("NVDA", "2024-01-02", 100.0, 102.0, 101.5, 1_000_000.0, 0.0)
    assert rows[2][6] == 0.25  # dividend on row 3


def test_yf_bars_idempotent(tmp_db):
    """Running twice does not duplicate rows (UNIQUE(ticker, ts))."""
    from etl.extract_yfinance import run_yf_bars_etl

    fake_ticker = MagicMock()
    fake_ticker.history.return_value = _fake_history_df()

    with patch("etl.extract_yfinance.yf.Ticker", return_value=fake_ticker), \
         patch("etl.extract_yfinance._RATE_DELAY", 0):
        run_yf_bars_etl(tickers=["NVDA"])
        run_yf_bars_etl(tickers=["NVDA"])

    with get_connection() as conn:
        n = conn.execute("SELECT COUNT(*) FROM staging_yf_bars").fetchone()[0]
    assert n == 3


def test_yf_bars_empty_history_is_logged_not_raised(tmp_db):
    """If yfinance returns an empty DataFrame, the ticker is skipped silently."""
    from etl.extract_yfinance import run_yf_bars_etl

    fake_ticker = MagicMock()
    fake_ticker.history.return_value = pd.DataFrame()

    with patch("etl.extract_yfinance.yf.Ticker", return_value=fake_ticker), \
         patch("etl.extract_yfinance._RATE_DELAY", 0):
        count = run_yf_bars_etl(tickers=["DELISTED"])

    assert count == 0
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `pytest tests/bronze/test_bronze_yfinance_bars.py -v`
Expected: the three new tests FAIL with `ModuleNotFoundError: No module named 'etl.extract_yfinance'`. The earlier `test_staging_tables_exist` still PASSES.

---

## Task 4: yfinance bars ETL — implementation

**Files:**
- Create: `etl/extract_yfinance.py`

- [ ] **Step 1: Create the module**

Create `etl/extract_yfinance.py` with this content:

```python
"""
etl/extract_yfinance.py
yfinance-backed ETL for the staging area.

Two entry points:
  run_yf_bars_etl    — daily bars for the 32 validation tickers (Mag 7 + semis)
  run_yf_indices_etl — daily bars for 18 major indices + derived spreads + stats

All output lands in ibkr.duckdb under tables prefixed `staging_yf_`.
"""
import time
from typing import Iterable, Optional

import pandas as pd
import yfinance as yf
from loguru import logger

from db.database import get_connection
from etl.bulk_load_massive import TICKERS as SEMI_TICKERS


# Sleep between yfinance calls — yfinance is unofficial and rate-limit-prone.
# Override in tests via patch("etl.extract_yfinance._RATE_DELAY", 0).
_RATE_DELAY = 0.5

# Index universe — 16 real symbols + 2 derived spreads computed at load time.
INDICES = [
    "ACWI",   # MSCI All Country
    "ACWX",   # MSCI All Country ex-US
    "^GSPC",  # S&P 500
    "SPDW",   # S&P Developed World ex-US ("S&P Rest of the World")
    "RSP",    # S&P 500 Equal Weight
    "^DJI",   # Dow Jones Industrial Average
    "^IXIC",  # Nasdaq Composite
    "SPTM",   # S&P 1500 Total Market
    "MDY",    # S&P MidCap 400
    "^SP600", # S&P SmallCap 600
    "^RUT",   # Russell 2000
    "SMH",    # Semiconductor ETF
    "IGV",    # Software ETF
    "EZU",    # MSCI Europe
    "EEM",    # MSCI Emerging Markets
    "EWJ",    # MSCI Japan
]

# Symbols for derived spreads. Written into staging_yf_indices alongside reals.
DERIVED_SPREADS = [
    # (output_symbol, left_symbol, right_symbol)  -- close = left.close - right.close
    ("^IXIC_MINUS_GSPC", "^IXIC", "^GSPC"),
    ("^RUT_MINUS_GSPC",  "^RUT",  "^GSPC"),
]


def _fetch_one(symbol: str) -> Optional[pd.DataFrame]:
    """Fetch full-history daily bars for one symbol with one retry."""
    for attempt in (1, 2):
        try:
            df = yf.Ticker(symbol).history(
                period="max", interval="1d", auto_adjust=False
            )
            if df is None or df.empty:
                logger.warning(f"yfinance returned empty df for {symbol}")
                return None
            return df
        except Exception as e:
            if attempt == 1:
                logger.warning(f"yfinance fetch failed for {symbol} (attempt 1): {e} — retrying")
                time.sleep(5)
                continue
            logger.error(f"yfinance fetch failed for {symbol} (attempt 2): {e}")
            return None
    return None


def _insert_bars(conn, table: str, symbol_col: str, symbol: str, df: pd.DataFrame) -> int:
    """Insert a yfinance df into the given staging table. Returns rows inserted."""
    rows = 0
    has_div = "Dividends" in df.columns
    has_split = "Stock Splits" in df.columns
    has_adj = "Adj Close" in df.columns

    for idx, row in df.iterrows():
        ts = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)[:10]
        if table == "staging_yf_bars":
            conn.execute(
                f"""
                INSERT OR IGNORE INTO {table}
                    ({symbol_col}, ts, open, high, low, close, adj_close,
                     volume, dividends, splits)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol, ts,
                    float(row["Open"])      if pd.notna(row["Open"])      else None,
                    float(row["High"])      if pd.notna(row["High"])      else None,
                    float(row["Low"])       if pd.notna(row["Low"])       else None,
                    float(row["Close"])     if pd.notna(row["Close"])     else None,
                    float(row["Adj Close"]) if has_adj and pd.notna(row["Adj Close"]) else None,
                    float(row["Volume"])    if pd.notna(row["Volume"])    else None,
                    float(row["Dividends"]) if has_div and pd.notna(row["Dividends"]) else 0.0,
                    float(row["Stock Splits"]) if has_split and pd.notna(row["Stock Splits"]) else 0.0,
                ),
            )
        else:  # staging_yf_indices — no dividends / splits columns
            conn.execute(
                f"""
                INSERT OR IGNORE INTO {table}
                    ({symbol_col}, ts, open, high, low, close, adj_close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol, ts,
                    float(row["Open"])      if pd.notna(row["Open"])      else None,
                    float(row["High"])      if pd.notna(row["High"])      else None,
                    float(row["Low"])       if pd.notna(row["Low"])       else None,
                    float(row["Close"])     if pd.notna(row["Close"])     else None,
                    float(row["Adj Close"]) if has_adj and pd.notna(row["Adj Close"]) else None,
                    float(row["Volume"])    if pd.notna(row["Volume"])    else None,
                ),
            )
        rows += 1
    return rows


def run_yf_bars_etl(tickers: Optional[Iterable[str]] = None) -> int:
    """Fetch daily bars for `tickers` (default = the 32 semi/Mag7 set) into staging_yf_bars."""
    tickers = list(tickers) if tickers is not None else sorted(SEMI_TICKERS)
    logger.info(f"yf-bars: {len(tickers)} tickers")
    total = 0
    with get_connection() as conn:
        for sym in tickers:
            df = _fetch_one(sym)
            if df is None:
                time.sleep(_RATE_DELAY)
                continue
            rows = _insert_bars(conn, "staging_yf_bars", "ticker", sym, df)
            conn.commit()
            total += rows
            logger.debug(f"yf-bars {sym}: {rows} rows")
            time.sleep(_RATE_DELAY)
    logger.info(f"yf-bars complete: {total:,} rows across {len(tickers)} tickers")
    return total
```

- [ ] **Step 2: Run Task 3 tests to verify they pass**

Run: `pytest tests/bronze/test_bronze_yfinance_bars.py -v`
Expected: all four tests PASS (`test_staging_tables_exist`, `test_yf_bars_writes_rows`, `test_yf_bars_idempotent`, `test_yf_bars_empty_history_is_logged_not_raised`).

- [ ] **Step 3: Commit**

```bash
git add etl/extract_yfinance.py tests/bronze/test_bronze_yfinance_bars.py
git commit -m "feat(etl): yfinance daily bars ETL into staging_yf_bars"
```

---

## Task 5: yfinance indices ETL — failing test

**Files:**
- Test: `tests/bronze/test_bronze_yfinance_indices.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/bronze/test_bronze_yfinance_indices.py`:

```python
"""
tests/bronze/test_bronze_yfinance_indices.py
Bronze layer: yfinance major-indices ingestion + derived spread symbols.
"""
from unittest.mock import patch, MagicMock
import pandas as pd

from db.database import get_connection


def _two_day_df(close_left: float, close_right: float):
    """Tiny 2-row df with matching dates for spread arithmetic."""
    idx = pd.to_datetime(["2024-01-02", "2024-01-03"])
    return pd.DataFrame(
        {
            "Open":      [100.0, 101.0],
            "High":      [105.0, 106.0],
            "Low":       [ 95.0,  96.0],
            "Close":     [close_left, close_left + 1.0],
            "Adj Close": [close_left, close_left + 1.0],
            "Volume":    [1_000_000, 1_100_000],
        },
        index=idx,
    )


def test_yf_indices_writes_each_symbol(tmp_db):
    """run_yf_indices_etl iterates INDICES and inserts rows per symbol."""
    from etl.extract_yfinance import run_yf_indices_etl, INDICES

    def fake_ticker_factory(sym):
        m = MagicMock()
        m.history.return_value = _two_day_df(close_left=200.0, close_right=0.0)
        return m

    with patch("etl.extract_yfinance.yf.Ticker", side_effect=fake_ticker_factory), \
         patch("etl.extract_yfinance._RATE_DELAY", 0), \
         patch("etl.extract_yfinance._compute_index_stats", return_value=0):
        run_yf_indices_etl()

    with get_connection() as conn:
        symbols = {
            r[0] for r in conn.execute(
                "SELECT DISTINCT symbol FROM staging_yf_indices"
            ).fetchall()
        }
    for sym in INDICES:
        assert sym in symbols, f"{sym} missing from staging_yf_indices"


def test_yf_indices_derived_spreads_are_correct(tmp_db):
    """Derived rows ^IXIC_MINUS_GSPC and ^RUT_MINUS_GSPC equal close_left - close_right."""
    from etl.extract_yfinance import run_yf_indices_etl

    closes_per_symbol = {
        "^IXIC": 16000.0,
        "^GSPC": 4800.0,
        "^RUT":  2100.0,
    }

    def fake_ticker_factory(sym):
        m = MagicMock()
        base = closes_per_symbol.get(sym, 100.0)
        idx = pd.to_datetime(["2024-01-02", "2024-01-03"])
        m.history.return_value = pd.DataFrame(
            {
                "Open":      [base, base + 1.0],
                "High":      [base + 5, base + 6],
                "Low":       [base - 5, base - 4],
                "Close":     [base, base + 1.0],
                "Adj Close": [base, base + 1.0],
                "Volume":    [1, 1],
            },
            index=idx,
        )
        return m

    with patch("etl.extract_yfinance.yf.Ticker", side_effect=fake_ticker_factory), \
         patch("etl.extract_yfinance._RATE_DELAY", 0), \
         patch("etl.extract_yfinance._compute_index_stats", return_value=0):
        run_yf_indices_etl()

    with get_connection() as conn:
        rows = conn.execute(
            "SELECT ts, close FROM staging_yf_indices "
            "WHERE symbol = '^IXIC_MINUS_GSPC' ORDER BY ts"
        ).fetchall()
        assert rows[0] == ("2024-01-02", 16000.0 - 4800.0)
        assert rows[1] == ("2024-01-03", 16001.0 - 4801.0)

        rut_rows = conn.execute(
            "SELECT close FROM staging_yf_indices "
            "WHERE symbol = '^RUT_MINUS_GSPC' ORDER BY ts"
        ).fetchall()
        assert rut_rows[0][0] == 2100.0 - 4800.0
        assert rut_rows[1][0] == 2101.0 - 4801.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/bronze/test_bronze_yfinance_indices.py -v`
Expected: both tests FAIL with `ImportError: cannot import name 'run_yf_indices_etl'` (function doesn't exist yet).

---

## Task 6: yfinance indices ETL — implementation

**Files:**
- Modify: `etl/extract_yfinance.py` (append below `run_yf_bars_etl`)

- [ ] **Step 1: Append the implementation**

In `etl/extract_yfinance.py`, append below `run_yf_bars_etl`:

```python
def _compute_derived_spreads(conn) -> int:
    """For each entry in DERIVED_SPREADS, insert rows into staging_yf_indices
    where close = left.close - right.close on matching dates.

    Returns total rows inserted.
    """
    total = 0
    for out_symbol, left, right in DERIVED_SPREADS:
        result = conn.execute(
            """
            SELECT l.ts,
                   l.open  - r.open  AS open,
                   l.high  - r.high  AS high,
                   l.low   - r.low   AS low,
                   l.close - r.close AS close,
                   l.adj_close - r.adj_close AS adj_close
            FROM staging_yf_indices l
            JOIN staging_yf_indices r ON l.ts = r.ts
            WHERE l.symbol = ? AND r.symbol = ?
            ORDER BY l.ts
            """,
            (left, right),
        ).fetchall()
        for ts, o, h, lo, c, adj in result:
            conn.execute(
                """
                INSERT OR IGNORE INTO staging_yf_indices
                    (symbol, ts, open, high, low, close, adj_close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (out_symbol, ts, o, h, lo, c, adj),
            )
            total += 1
    conn.commit()
    return total


def run_yf_indices_etl(symbols: Optional[Iterable[str]] = None) -> int:
    """Fetch full-history daily bars for INDICES, materialise derived spreads,
    then recompute staging_yf_index_stats.

    Returns total rows touched (raw + derived + stats).
    """
    symbols = list(symbols) if symbols is not None else list(INDICES)
    logger.info(f"yf-indices: {len(symbols)} real symbols + {len(DERIVED_SPREADS)} derived")
    raw_total = 0
    with get_connection() as conn:
        for sym in symbols:
            df = _fetch_one(sym)
            if df is None:
                time.sleep(_RATE_DELAY)
                continue
            rows = _insert_bars(conn, "staging_yf_indices", "symbol", sym, df)
            conn.commit()
            raw_total += rows
            logger.debug(f"yf-indices {sym}: {rows} rows")
            time.sleep(_RATE_DELAY)

        derived = _compute_derived_spreads(conn)
        logger.info(f"yf-indices derived spreads: {derived} rows")

        stats = _compute_index_stats(conn)
        logger.info(f"yf-indices stats recomputed: {stats} rows")

    total = raw_total + derived + stats
    logger.info(f"yf-indices complete: {total:,} rows total")
    return total


def _compute_index_stats(conn) -> int:
    """Placeholder — real implementation lands in Task 8.
    Returning 0 here keeps Task 6 tests green; Task 8 replaces this body.
    """
    return 0
```

- [ ] **Step 2: Run Task 5 tests to verify they pass**

Run: `pytest tests/bronze/test_bronze_yfinance_indices.py -v`
Expected: both tests PASS.

- [ ] **Step 3: Run the full bronze suite to verify no regressions**

Run: `pytest tests/bronze -v`
Expected: all bronze tests PASS (including the pre-existing polygon bronze tests).

- [ ] **Step 4: Commit**

```bash
git add etl/extract_yfinance.py tests/bronze/test_bronze_yfinance_indices.py
git commit -m "feat(etl): yfinance indices ETL + derived spread symbols"
```

---

## Task 7: Index statistics — failing test

**Files:**
- Test: `tests/silver/test_silver_index_stats.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/silver/test_silver_index_stats.py`:

```python
"""
tests/silver/test_silver_index_stats.py
Silver layer: derived statistics computed from staging_yf_indices.
"""
import math
from datetime import date, timedelta

from db.database import get_connection


def _seed_constant_price_series(conn, symbol: str, n_days: int, price: float):
    """Insert n_days of constant-price bars. Useful for trivial closed-form checks."""
    start = date(2020, 1, 1)
    for i in range(n_days):
        ts = (start + timedelta(days=i)).isoformat()
        conn.execute(
            """
            INSERT INTO staging_yf_indices
                (symbol, ts, open, high, low, close, adj_close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (symbol, ts, price, price, price, price, price),
        )
    conn.commit()


def test_index_stats_constant_series_has_zero_returns_and_vol(tmp_db):
    """A constant price series → all returns 0, vol 0, drawdown 0, sigma-bands all equal mean."""
    from etl.extract_yfinance import _compute_index_stats

    with get_connection() as conn:
        _seed_constant_price_series(conn, "FLAT", n_days=300, price=100.0)
        _compute_index_stats(conn)

        row = conn.execute(
            "SELECT ret_1d, vol_252d, drawdown, max_drawdown_to_date, "
            "       mean_252d, sigma_252d, band_plus_1, band_minus_4 "
            "FROM staging_yf_index_stats "
            "WHERE symbol = 'FLAT' "
            "ORDER BY ts DESC LIMIT 1"
        ).fetchone()

    ret_1d, vol_252d, dd, max_dd, mean, sigma, band_p1, band_m4 = row
    assert abs(ret_1d) < 1e-9
    assert vol_252d is not None and abs(vol_252d) < 1e-9
    assert dd is not None and abs(dd) < 1e-9
    assert max_dd is not None and abs(max_dd) < 1e-9
    assert mean is not None and abs(mean) < 1e-9
    assert sigma is not None and abs(sigma) < 1e-9
    # mean ± kσ all collapse to 0 when sigma is 0
    assert abs(band_p1) < 1e-9
    assert abs(band_m4) < 1e-9


def test_index_stats_drawdown_detects_peak_to_trough(tmp_db):
    """100 → 80 then back up → max_drawdown_to_date = -0.20."""
    from etl.extract_yfinance import _compute_index_stats

    with get_connection() as conn:
        # 50 days at 100, 50 days at 80, 50 days at 90 (still below peak)
        start = date(2020, 1, 1)
        prices = [100.0] * 50 + [80.0] * 50 + [90.0] * 50
        for i, p in enumerate(prices):
            ts = (start + timedelta(days=i)).isoformat()
            conn.execute(
                """
                INSERT INTO staging_yf_indices
                    (symbol, ts, open, high, low, close, adj_close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                ("DD", ts, p, p, p, p, p),
            )
        conn.commit()
        _compute_index_stats(conn)

        max_dd = conn.execute(
            "SELECT max_drawdown_to_date FROM staging_yf_index_stats "
            "WHERE symbol = 'DD' ORDER BY ts DESC LIMIT 1"
        ).fetchone()[0]

    assert max_dd is not None
    assert abs(max_dd - (-0.20)) < 1e-6, f"expected -0.20, got {max_dd}"


def test_index_stats_short_history_returns_null_long_windows(tmp_db):
    """A symbol with only 30 days of data has NULL in vol_252d / mean_252d / bands."""
    from etl.extract_yfinance import _compute_index_stats

    with get_connection() as conn:
        _seed_constant_price_series(conn, "SHORT", n_days=30, price=50.0)
        _compute_index_stats(conn)

        row = conn.execute(
            "SELECT vol_252d, mean_252d, sigma_252d, band_plus_2 "
            "FROM staging_yf_index_stats "
            "WHERE symbol = 'SHORT' ORDER BY ts DESC LIMIT 1"
        ).fetchone()

    vol_252d, mean_252d, sigma_252d, band_plus_2 = row
    assert vol_252d is None
    assert mean_252d is None
    assert sigma_252d is None
    assert band_plus_2 is None


def test_index_stats_rebuild_is_idempotent(tmp_db):
    """Running _compute_index_stats twice does not double rows."""
    from etl.extract_yfinance import _compute_index_stats

    with get_connection() as conn:
        _seed_constant_price_series(conn, "IDEM", n_days=100, price=10.0)
        _compute_index_stats(conn)
        n1 = conn.execute("SELECT COUNT(*) FROM staging_yf_index_stats").fetchone()[0]
        _compute_index_stats(conn)
        n2 = conn.execute("SELECT COUNT(*) FROM staging_yf_index_stats").fetchone()[0]

    assert n1 == n2 == 100
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/silver/test_silver_index_stats.py -v`
Expected: all four tests FAIL — currently `_compute_index_stats` returns 0 and writes nothing.

---

## Task 8: Index statistics — implementation

**Files:**
- Modify: `etl/extract_yfinance.py` — replace the placeholder `_compute_index_stats` body

- [ ] **Step 1: Replace the placeholder**

In `etl/extract_yfinance.py`, replace the entire `_compute_index_stats` function (placeholder added in Task 6) with:

```python
def _compute_index_stats(conn) -> int:
    """Rebuild staging_yf_index_stats from scratch using DuckDB window functions.

    Annualisation factor: 252 trading days.
    Sharpe assumes risk-free = 0 (note in README).
    Long-window stats (252d) return NULL where history is insufficient.

    Returns rows written.
    """
    conn.execute("DELETE FROM staging_yf_index_stats")

    # Step 1: build per-day log returns and running max close (for drawdown).
    # Step 2: roll up vol / mean / sigma / Sharpe / skew / kurt over windows.
    # All in a single INSERT … SELECT with window functions.
    conn.execute(
        """
        INSERT INTO staging_yf_index_stats (
            symbol, ts,
            ret_1d, ret_21d, ret_252d,
            vol_21d, vol_63d, vol_252d,
            drawdown, max_drawdown_to_date,
            sharpe_252d, skew_252d, kurt_252d,
            mean_252d, sigma_252d,
            band_plus_1, band_minus_1,
            band_plus_2, band_minus_2,
            band_plus_3, band_minus_3,
            band_plus_4, band_minus_4
        )
        WITH base AS (
            SELECT
                symbol,
                ts,
                close,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts) AS rn,
                LN(close / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY ts), 0)) AS r,
                MAX(close) OVER (PARTITION BY symbol ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_peak
            FROM staging_yf_indices
            WHERE close IS NOT NULL AND close > 0
        ),
        rolled AS (
            SELECT
                symbol, ts, close, rn, r, running_peak,
                AVG(r) OVER w21  AS mean_21,
                AVG(r) OVER w63  AS mean_63,
                AVG(r) OVER w252 AS mean_252,
                STDDEV_SAMP(r) OVER w21  AS sd_21,
                STDDEV_SAMP(r) OVER w63  AS sd_63,
                STDDEV_SAMP(r) OVER w252 AS sd_252,
                SKEWNESS(r)   OVER w252 AS sk_252,
                KURTOSIS(r)   OVER w252 AS ku_252,
                SUM(r) OVER w21  AS sumr_21,
                SUM(r) OVER w252 AS sumr_252,
                COUNT(r) OVER w252 AS n_252
            FROM base
            WINDOW
                w21  AS (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 20  PRECEDING AND CURRENT ROW),
                w63  AS (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 62  PRECEDING AND CURRENT ROW),
                w252 AS (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 251 PRECEDING AND CURRENT ROW)
        )
        SELECT
            symbol,
            ts,
            r                                                   AS ret_1d,
            CASE WHEN rn >= 21  THEN sumr_21  END                AS ret_21d,
            CASE WHEN rn >= 252 THEN sumr_252 END                AS ret_252d,
            CASE WHEN rn >= 21  THEN sd_21  * SQRT(252) END      AS vol_21d,
            CASE WHEN rn >= 63  THEN sd_63  * SQRT(252) END      AS vol_63d,
            CASE WHEN n_252 >= 252 THEN sd_252 * SQRT(252) END   AS vol_252d,
            (close / NULLIF(running_peak, 0)) - 1.0              AS drawdown,
            MIN((close / NULLIF(running_peak, 0)) - 1.0)
                OVER (PARTITION BY symbol ORDER BY ts
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                                                 AS max_drawdown_to_date,
            CASE WHEN n_252 >= 252 AND sd_252 > 0
                 THEN (mean_252 * 252) / (sd_252 * SQRT(252))
            END                                                  AS sharpe_252d,
            CASE WHEN n_252 >= 252 THEN sk_252 END               AS skew_252d,
            CASE WHEN n_252 >= 252 THEN ku_252 END               AS kurt_252d,
            CASE WHEN n_252 >= 252 THEN mean_252 END             AS mean_252d,
            CASE WHEN n_252 >= 252 THEN sd_252   END             AS sigma_252d,
            CASE WHEN n_252 >= 252 THEN mean_252 + 1 * sd_252 END AS band_plus_1,
            CASE WHEN n_252 >= 252 THEN mean_252 - 1 * sd_252 END AS band_minus_1,
            CASE WHEN n_252 >= 252 THEN mean_252 + 2 * sd_252 END AS band_plus_2,
            CASE WHEN n_252 >= 252 THEN mean_252 - 2 * sd_252 END AS band_minus_2,
            CASE WHEN n_252 >= 252 THEN mean_252 + 3 * sd_252 END AS band_plus_3,
            CASE WHEN n_252 >= 252 THEN mean_252 - 3 * sd_252 END AS band_minus_3,
            CASE WHEN n_252 >= 252 THEN mean_252 + 4 * sd_252 END AS band_plus_4,
            CASE WHEN n_252 >= 252 THEN mean_252 - 4 * sd_252 END AS band_minus_4
        FROM rolled
        """
    )

    n = conn.execute("SELECT COUNT(*) FROM staging_yf_index_stats").fetchone()[0]
    conn.commit()
    return n
```

- [ ] **Step 2: Run Task 7 tests to verify they pass**

Run: `pytest tests/silver/test_silver_index_stats.py -v`
Expected: all four tests PASS.

- [ ] **Step 3: Run full test suite to confirm no regressions**

Run: `pytest -v`
Expected: all tests PASS (silver + bronze + everything pre-existing).

- [ ] **Step 4: Commit**

```bash
git add etl/extract_yfinance.py tests/silver/test_silver_index_stats.py
git commit -m "feat(etl): compute yfinance index stats (returns/vol/drawdown/sigma bands)"
```

---

## Task 9: Polygon daily flat-file loader — failing test

**Files:**
- Test: `tests/bronze/test_bronze_bulk_load_daily.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/bronze/test_bronze_bulk_load_daily.py`:

```python
"""
tests/bronze/test_bronze_bulk_load_daily.py
Bronze layer: Polygon day_aggs_v1 flat-file loader.
"""
import gzip
from pathlib import Path

from db.database import get_connection


def _write_fake_day_aggs_gz(path: Path, rows: list[dict]):
    """Write a Polygon-style day_aggs CSV.gz with given rows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    header = "ticker,volume,open,close,high,low,window_start,transactions"
    body = "\n".join(
        f"{r['ticker']},{r['volume']},{r['open']},{r['close']},"
        f"{r['high']},{r['low']},{r['window_start']},{r['transactions']}"
        for r in rows
    )
    with gzip.open(path, "wt") as f:
        f.write(header + "\n" + body + "\n")


def test_bulk_load_daily_filters_to_tickers(tmp_path, tmp_db, monkeypatch):
    """Only rows whose ticker is in the TICKERS set land in polygon_bars."""
    from etl import bulk_load_daily

    download_dir = tmp_path / "day_aggs" / "2024"
    fake_file = download_dir / "2024-01-02.csv.gz"
    # window_start in nanoseconds, 2024-01-02 14:30:00 UTC
    ns = 1704206400 * 1_000_000_000
    _write_fake_day_aggs_gz(
        fake_file,
        rows=[
            {"ticker": "NVDA",  "volume": 1000, "open": 100.0, "close": 105.0,
             "high": 106.0, "low": 99.0, "window_start": ns, "transactions": 50},
            {"ticker": "BOGUS", "volume": 9999, "open": 1.0, "close": 1.0,
             "high": 1.0, "low": 1.0, "window_start": ns, "transactions": 1},
            {"ticker": "AMD",   "volume": 2000, "open": 200.0, "close": 210.0,
             "high": 212.0, "low": 198.0, "window_start": ns, "transactions": 80},
        ],
    )

    monkeypatch.setattr(bulk_load_daily, "DOWNLOAD_DIR", tmp_path / "day_aggs")
    rows_written = bulk_load_daily.run(start_year=2024, end_year=2024, skip_download=True)

    assert rows_written == 2, f"expected 2 (NVDA + AMD), got {rows_written}"

    with get_connection() as conn:
        tickers = {
            r[0] for r in conn.execute(
                "SELECT DISTINCT ticker FROM polygon_bars WHERE timespan = 'day'"
            ).fetchall()
        }
        assert tickers == {"NVDA", "AMD"}

        # Verify NVDA OHLCV survived correctly
        nvda = conn.execute(
            "SELECT open, high, low, close, volume FROM polygon_bars "
            "WHERE ticker='NVDA' AND timespan='day'"
        ).fetchone()
        assert nvda == (100.0, 106.0, 99.0, 105.0, 1000.0)


def test_bulk_load_daily_idempotent(tmp_path, tmp_db, monkeypatch):
    """Loading the same file twice does not duplicate rows."""
    from etl import bulk_load_daily

    download_dir = tmp_path / "day_aggs" / "2024"
    fake_file = download_dir / "2024-01-02.csv.gz"
    ns = 1704206400 * 1_000_000_000
    _write_fake_day_aggs_gz(
        fake_file,
        rows=[
            {"ticker": "NVDA", "volume": 1000, "open": 100.0, "close": 105.0,
             "high": 106.0, "low": 99.0, "window_start": ns, "transactions": 50},
        ],
    )

    monkeypatch.setattr(bulk_load_daily, "DOWNLOAD_DIR", tmp_path / "day_aggs")
    bulk_load_daily.run(start_year=2024, end_year=2024, skip_download=True)
    bulk_load_daily.run(start_year=2024, end_year=2024, skip_download=True)

    with get_connection() as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM polygon_bars WHERE timespan='day'"
        ).fetchone()[0]
    assert n == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/bronze/test_bronze_bulk_load_daily.py -v`
Expected: tests FAIL with `ImportError: No module named 'etl.bulk_load_daily'`.

---

## Task 10: Polygon daily flat-file loader — implementation

**Files:**
- Create: `etl/bulk_load_daily.py`

- [ ] **Step 1: Create the module**

Create `etl/bulk_load_daily.py`:

```python
"""
etl/bulk_load_daily.py
Download daily-bar flat files from Polygon S3 and load the configured tickers
into polygon_bars with timespan='day'.

Sibling of etl/bulk_load_massive.py (which loads MINUTE bars). Kept separate
on purpose — do not refactor the two together.

Usage:
    python -m etl.bulk_load_daily --start 2021 --end 2026
    python -m etl.bulk_load_daily --start 2021 --end 2026 --skip-download
"""
import argparse
import csv
import gzip
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

from db.database import get_connection
from etl.bulk_load_massive import TICKERS  # SAME 32 tickers, single source of truth

S3_ENDPOINT  = "https://files.massive.com"
S3_BUCKET    = "s3://flatfiles/us_stocks_sip/day_aggs_v1"
AWS_PROFILE  = "massive"
AWS_CLI      = r"C:\Program Files\Amazon\AWSCLIV2\aws.exe"
DOWNLOAD_DIR = Path("data/day_aggs")


def _ns_to_iso(ns: int) -> str:
    """Convert nanosecond timestamp to ISO-8601 UTC string."""
    return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc).isoformat(timespec="seconds")


def download_year(year: int):
    """Sync all daily files for a given year from S3."""
    dest = DOWNLOAD_DIR / str(year)
    dest.mkdir(parents=True, exist_ok=True)
    logger.info(f"Syncing {S3_BUCKET}/{year}/ → {dest}")
    subprocess.run([
        AWS_CLI, "s3", "sync", f"{S3_BUCKET}/{year}/", str(dest),
        "--endpoint-url", S3_ENDPOINT,
        "--profile", AWS_PROFILE,
        "--no-progress",
    ])


def load_file(path: Path, conn) -> int:
    """Load rows for tickers in TICKERS from a day_aggs csv.gz. Returns rows inserted."""
    rows = 0
    with gzip.open(path, "rt") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["ticker"] not in TICKERS:
                continue
            try:
                ts = _ns_to_iso(int(row["window_start"]))
                conn.execute(
                    """
                    INSERT OR IGNORE INTO polygon_bars
                        (ticker, ts, timespan, open, high, low, close, volume, transactions)
                    VALUES (?, ?, 'day', ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["ticker"], ts,
                        float(row["open"]),
                        float(row["high"]),
                        float(row["low"]),
                        float(row["close"]),
                        float(row["volume"]),
                        int(row["transactions"]) if row["transactions"] else None,
                    ),
                )
                rows += 1
            except Exception as e:
                logger.warning(f"Skipping row in {path.name}: {e}")
    conn.commit()
    return rows


def run(start_year: int = 2021, end_year: int = 2026, skip_download: bool = False) -> int:
    """Programmatic entry point. Returns total rows inserted."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    if not skip_download:
        for year in range(start_year, end_year + 1):
            download_year(year)

    files = sorted(
        f for f in DOWNLOAD_DIR.rglob("*.csv.gz")
        if start_year <= int(f.parent.name) <= end_year
    )

    logger.info(f"bulk-load-daily: {len(files)} files, filtering to {len(TICKERS)} tickers")
    total = 0
    with get_connection() as conn:
        for i, path in enumerate(files, 1):
            total += load_file(path, conn)
            if i % 50 == 0 or i == len(files):
                logger.info(f"bulk-load-daily: {i}/{len(files)} files — {total:,} rows")
    logger.info(f"bulk-load-daily done. {total:,} rows across {len(TICKERS)} tickers")
    return total


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=2021)
    parser.add_argument("--end",   type=int, default=2026)
    parser.add_argument("--skip-download", action="store_true")
    args = parser.parse_args()
    run(start_year=args.start, end_year=args.end, skip_download=args.skip_download)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run Task 9 tests to verify they pass**

Run: `pytest tests/bronze/test_bronze_bulk_load_daily.py -v`
Expected: both tests PASS.

- [ ] **Step 3: Run full bronze suite for regressions**

Run: `pytest tests/bronze -v`
Expected: all bronze tests PASS.

- [ ] **Step 4: Commit**

```bash
git add etl/bulk_load_daily.py tests/bronze/test_bronze_bulk_load_daily.py
git commit -m "feat(etl): Polygon day_aggs_v1 flat-file loader"
```

---

## Task 11: Wire the three new jobs into `main.py`

**Files:**
- Modify: `main.py` (header docstring, lazy imports, three `@etl_job` functions, argparse choices, polygon_only_jobs dict)

- [ ] **Step 1: Update the module docstring**

In `main.py`, update the docstring block at the top. Find the line:

```
    python main.py --job polygon-semis   # day bars + ticks for group-filtered tickers
```

Immediately below it (still in the docstring), insert:

```
    python main.py --job yf-bars             # yfinance daily bars for the 32 semi/Mag7 tickers (staging)
    python main.py --job yf-indices          # yfinance bars + stats for 18 major indices (staging)
    python main.py --job polygon-daily-flat  # Polygon S3 day_aggs_v1 flat-file load (additive to bulk_load_massive minute loader)
```

- [ ] **Step 2: Add the lazy imports**

In `main.py`, find the existing block of lazy imports (starts with `from db.database import init_db, get_connection`). At the end of that block (just after `from etl.utils import utcnow as _utcnow`), add:

```python
from etl.extract_yfinance import run_yf_bars_etl, run_yf_indices_etl
from etl import bulk_load_daily
```

- [ ] **Step 3: Add the three job functions**

In `main.py`, find the existing `@etl_job("cot")` block. Immediately after `def job_cot(): ...`, add:

```python
@etl_job("yf-bars")
def job_yf_bars():
    return run_yf_bars_etl()


@etl_job("yf-indices")
def job_yf_indices():
    return run_yf_indices_etl()


@etl_job("polygon-daily-flat")
def job_polygon_daily_flat():
    """Polygon day_aggs_v1 S3 flat-file load — additive to bulk_load_massive (minute)."""
    start_y = int(POLYGON_START_DATE[:4]) if POLYGON_START_DATE else 2021
    end_y   = int(POLYGON_END_DATE[:4])   if POLYGON_END_DATE   else 2026
    return bulk_load_daily.run(start_year=start_y, end_year=end_y)
```

- [ ] **Step 4: Add the three argparse choices**

In `main.py`, find the `argparse` `choices=[...]` list inside `main()`. The existing list ends with `"edgar-filings", "edgar-facts", "edgar-13f", "cot",`. Replace that final line with:

```python
                            "edgar-filings", "edgar-facts", "edgar-13f", "cot",
                            "yf-bars", "yf-indices", "polygon-daily-flat",
```

- [ ] **Step 5: Add the three entries to the `polygon_only_jobs` dict**

In `main.py`, find the `polygon_only_jobs = { ... }` dict. After the final entry (`"cot": job_cot,`), add:

```python
        "yf-bars":              job_yf_bars,
        "yf-indices":           job_yf_indices,
        "polygon-daily-flat":   job_polygon_daily_flat,
```

- [ ] **Step 6: Verify CLI help renders the new jobs**

Run: `python main.py --help`
Expected: stdout shows `yf-bars`, `yf-indices`, `polygon-daily-flat` in the `--job` choices list with no traceback.

- [ ] **Step 7: Verify each new job is dispatchable (smoke test using yfinance mock)**

Run: `python -c "import main; print('imports ok')"`
Expected: `imports ok` with no traceback.

- [ ] **Step 8: Commit**

```bash
git add main.py
git commit -m "feat(main): wire yf-bars, yf-indices, polygon-daily-flat jobs"
```

---

## Task 12: Update README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add the three jobs under the "Polygon.io" ETL Jobs section**

In `README.md`, find the block under `# ── Polygon.io (no TWS needed) ─` that ends with `python main.py --job polygon              # all polygon jobs`. Immediately after that line, add:

```
python main.py --job polygon-daily-flat   # Polygon day_aggs_v1 S3 flat-file load (additive — minute loader untouched)
```

- [ ] **Step 2: Add a new "yfinance (staging)" section under the ETL Jobs heading**

Below the Polygon block and above the `# ── SEC EDGAR ─` block in `README.md`, insert:

```
# ── yfinance (no API key needed — staging area) ──────────────────────────────
python main.py --job yf-bars              # daily bars for 32 semi/Mag7 tickers → staging_yf_bars
python main.py --job yf-indices           # daily bars + stats for 18 major indices → staging_yf_indices(+stats)
```

- [ ] **Step 3: Add the three staging tables to the Database Schema table**

In `README.md`, find the Database Schema markdown table (the one starting `| Table | Rows (approx) | Description |`). Before the final `| etl_runs ` row, insert these three rows:

```
| `staging_yf_bars` | ~5,000/ticker | yfinance daily OHLC+adj for the 32 validation tickers — for cross-checking polygon_bars |
| `staging_yf_indices` | ~6,500/index | yfinance daily bars for 18 major indices (incl. 2 derived spreads) |
| `staging_yf_index_stats` | same as indices | Returns/vol/drawdown + 1σ–4σ bands per index per day, rebuilt each run |
```

- [ ] **Step 4: Verify README renders without obvious breakage**

Run: `python -c "open('README.md').read()" && echo ok`
Expected: prints `ok` (file is readable).

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs: document yf-bars, yf-indices, polygon-daily-flat jobs and staging tables"
```

---

## Task 13: End-to-end smoke + push `claude`

**Pre-condition:** Tasks 1–12 were executed on the `claude` branch (the branch the spec + this plan live on). Verify with `git branch --show-current` → should print `claude`. If not, stop and switch branches before continuing.

**Files:**
- No code changes — verification + push only.

- [ ] **Step 1: Run the full test suite**

Run: `pytest -v`
Expected: all tests PASS. If anything is red, fix it before pushing.

- [ ] **Step 2: Confirm hard constraints were respected (zero diff vs `main` for protected files)**

Run: `git diff origin/main -- etl/bulk_load_massive.py etl/extract_polygon_ticks.py`
Expected: **no output** (zero lines changed in either file). If any output appears, revert those changes with `git checkout origin/main -- etl/bulk_load_massive.py etl/extract_polygon_ticks.py` and re-test.

- [ ] **Step 3: Verify the `polygon_bars` schema is unchanged**

Run:
```bash
python -c "from db.database import init_db, get_connection; init_db()
with get_connection() as conn:
    cols = [r[1] for r in conn.execute('PRAGMA table_info(polygon_bars)').fetchall()]
    print(cols)"
```
Expected: prints a list including `ticker, ts, timespan, open, high, low, close, volume, vwap, transactions, created_at` — schema unchanged from `main`.

- [ ] **Step 4: Push `claude`**

Run: `git push origin claude`
Expected: push succeeds. (Branch already exists upstream — no `-u` needed.)

- [ ] **Step 5: Confirm spec + plan + code are all on the branch**

Run:
```bash
git fetch origin
git log --oneline origin/claude ^origin/main | head -20
git ls-tree -r origin/claude --name-only | grep -E "(extract_yfinance|bulk_load_daily|2026-06-06-yfinance)"
```
Expected: the new commits are listed; the grep prints both the spec and plan paths plus `etl/extract_yfinance.py` and `etl/bulk_load_daily.py`.

---

## Done state

- 7 new commits on `claude` branch implementing the spec
- All tests pass
- `etl/bulk_load_massive.py` and `etl/extract_polygon_ticks.py` byte-identical to `main`
- Spec + plan committed and visible on the branch for mimo to read in opencode
- Sonnet can review the PR from `claude` → `main` when ready
