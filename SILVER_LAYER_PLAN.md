# Silver Layer Plan

Medallion position: **Bronze** (raw Polygon flat-file bars, as-ingested) → **Silver** (cleaned, typed, feature-enriched, analytics-ready) → **Gold** (marts / model features).

This document defines the Silver layer for the semiconductor universe (34 tickers) and the two Silver tables requested:

1. `silver_stock_features` — per-stock daily technical features (moving averages, rolling std, z-score, VWAP).
2. `silver_option_greeks` — per-contract daily option greeks (Black-Scholes-Merton).

---

## 1. Design principles

- **Grain:** one row per entity per trading day. Stocks keyed on `(ticker, ts)`; options on `(option_ticker, ts)`.
- **Derived, reproducible, idempotent:** Silver is fully recomputable from Bronze. Rebuilds use `INSERT OR REPLACE` (unlike Bronze which is append/`IGNORE`) — Silver is a *computed view of state*, not an immutable capture.
- **Typed timestamps as a Silver concern:** Bronze stores `ts` as ISO-8601 **text**. Silver keeps `ts` as text for join-compatibility but adds a typed `trade_date DATE` for correct window ordering and range scans.
- **Naming:** `silver_` prefix in the `main` schema (matches existing Bronze convention; no DuckDB schema namespaces introduced). See "Open decision D5".

---

## 2. `silver_stock_features`

**Source:** `polygon_bars WHERE timespan = 'day'` (Bronze, 5yr, 34 tickers, already populated).

**Grain:** one row per `(ticker, trade_date)`.

**Feature definitions** (all windows are trailing, min-periods = full window; partial windows → NULL):

| Feature | Formula | Notes |
|---------|---------|-------|
| `daily_return` | `close / lag(close) - 1` | simple daily return |
| `ma_20/50/100` | `avg(close)` over trailing N days | simple moving average |
| `std_20/50/100` | `stddev_samp(close)` over trailing N days | rolling sample std of **close** (see D2) |
| `zscore_20/50/100` | `(close - ma_N) / std_N` | price z-score; how many σ price sits from its N-day mean |
| `vwap_20/50/100` | `sum(typical_price × volume) / sum(volume)` over trailing N days, where `typical_price = (high+low+close)/3` | rolling VWAP (see D1) |

**Computation:** single DuckDB pass using window functions partitioned by ticker, ordered by `trade_date`:

```sql
avg(close)        OVER w20  AS ma_20,
stddev_samp(close) OVER w20 AS std_20,
...
WINDOW w20 AS (PARTITION BY ticker ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
```

z-score and VWAP are computed from the windowed aggregates. Rows where the full window isn't available are emitted with NULL features (kept for continuity) or filtered — see D3.

---

## 3. `silver_option_greeks`

**Sources:** `polygon_option_bars` (Bronze option OHLCV — the option market price) JOINed to `polygon_bars` (Bronze underlying daily close) on `(underlying = ticker, ts)`.

**Grain:** one row per `(option_ticker, trade_date)`.

**Method — Black-Scholes-Merton:** for each contract-day we know S (underlying close), K (strike), T (years to expiry), the option market price (option `close`), and assume r (risk-free) and q (dividend yield). We:
1. **Solve implied volatility** σ by inverting BSM against the option's market close (Newton/Brent).
2. **Compute greeks** analytically from (S, K, T, r, q, σ).

**Columns / greeks (the full standard set):**

| Column | Meaning |
|--------|---------|
| `und_close` | underlying close (S) |
| `time_to_expiry` | years to expiry (T), ACT/365 |
| `moneyness` | `und_close / strike` |
| `risk_free_rate` | r assumption used (D4) |
| `dividend_yield` | q assumption used (default 0) |
| `implied_vol` | σ solved from the option's market price |
| `delta` | ∂V/∂S |
| `gamma` | ∂²V/∂S² |
| `theta` | ∂V/∂t (per calendar day) |
| `vega` | ∂V/∂σ (per 1 vol point) |
| `rho` | ∂V/∂r |

Higher-order greeks (vanna, vomma, charm) are out of scope for v1 but the table can be extended.

**Implementation lib:** `py_vollib` / `py_vollib_vectorized` (BSM IV + greeks), or a self-contained SciPy implementation. To be selected at build time.

---

## 4. Dependencies & blockers

| Item | Status |
|------|--------|
| `silver_stock_features` source (Bronze day bars) | ✅ Ready — 42,573 rows, 34 tickers, 2021–2026 |
| `silver_option_greeks` source (Bronze option bars) | 🔴 **Blocked** — `us_options_opra` flat files are not entitled on the current Massive/Polygon plan (403). Schema can be created now; population waits on the options entitlement (or an accepted API path). |
| Risk-free rate source | ⚠️ Open (D4) |
| BSM library choice | ⚠️ Decide at build |

---

## 5. Open decisions (need your call)

- **D1 — VWAP:** you listed "vwap" once. I've specced rolling VWAP at 20/50/100 (consistent with the MAs) using typical price. Prefer a single window (e.g. 20d), or a session/anchored VWAP instead? (Session VWAP isn't computable from daily bars — would need minute bars.)
- **D2 — std / z-score basis:** rolling std of **close** (price z-score, mean-reversion) vs std of **returns** (volatility). I chose price-close. Confirm.
- **D3 — partial windows:** emit NULL features for the first N-1 days per ticker (keep row) vs drop those rows. I lean keep-with-NULL.
- **D4 — risk-free rate:** constant (e.g. 4.5%) vs a real curve (FRED DGS3MO/treasury). Constant is simplest for v1.
- **D5 — layering:** `silver_` table prefix in `main` (current choice) vs a dedicated DuckDB `silver` schema (`silver.stock_features`). Prefix is less disruptive.

---

## 6. Implementation sequence (after schemas land)

1. `etl/silver_stock_features.py` — DuckDB window-function build → `silver_stock_features` (buildable now).
2. `etl/silver_option_greeks.py` — BSM IV + greeks → `silver_option_greeks` (once options Bronze exists).
3. Extend `etl/export_hf.py` to publish Silver tables alongside Bronze.

Build via the established **Mimo → DeepSeek → Claude** flow.
