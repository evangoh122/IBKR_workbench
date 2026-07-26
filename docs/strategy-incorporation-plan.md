# Strategy Incorporation and Whole-Market Data Plan

**Status:** Planning only; no strategy or production data code is part of this change  
**Target cadence:** Hourly and daily bars only  
**Target universe:** The point-in-time whole U.S. equity and listed-equity-options market, not a fixed watchlist  
**Priority:** Equities and equity options first

## 1. Executive decision

IBKR_workbench should not begin by copying individual reference scripts. It first needs a point-in-time, whole-market data lake and a small, deterministic research engine. The reference scripts are useful as strategy specifications, but they assume one or a few symbols, frictionless fills, and in several cases data that this repository does not ingest.

The recommended first strategy tranche is:

1. Daily cross-sectional equity momentum/trend (MACD or moving-average state plus liquidity/risk filters).
2. Daily Donchian/Turtle breakout.
3. Daily or hourly Bollinger/RSI mean reversion.
4. Daily Heikin-Ashi or Parabolic SAR trend confirmation.
5. Daily pairs trading, after a scalable candidate-selection stage exists.
6. Daily options put/call-ratio (PCR) sentiment and liquid straddle research.
7. Daily volatility-risk-premium/IV-versus-realized-volatility strategies, after option quotes, Greeks, and point-in-time chains are complete.

This ordering deliberately favors signals that can be evaluated across the full market with OHLCV already close to hand. Options structures follow once the option flat-file path replaces the current per-underlying/per-contract download loop.

## 2. Survey method and scope

The three repositories were cloned and inspected at these revisions:

| Repository | Inspected revision | What was treated as source |
|---|---|---|
| `je-suis-tm/quant-trading` | `611b73f2c3f577ac5b28aaa19ac8c43d3236c7a5` | README, top-level backtest scripts, project READMEs/data |
| `wilsonfreitas/awesome-quant` | `200880bb0828cb9347557cea8bb5b04f2911ccae` | README curated index; this repository is an index, not a strategy implementation |
| `PyPatel/Options-Trading-Strategies-in-Python` | `c7b8a0de9b7232a1615a02acbed70bf2959fbe44` | All Python/C++ files and README |

“Suitable” below means the technique can operate on completed hourly or daily observations. “Exclude” means tick/order-book/latency dependent, non-trading analytics, an unrelated asset/data project, or a technique whose implementation is explicitly minute-level. Exclusion is from the first strategy program, not a claim that the project has no research value.

## 3. Reference-repository findings

### 3.1 `je-suis-tm/quant-trading`: implemented strategies and techniques

| Item found in source | Concrete technique | Cadence classification | Decision |
|---|---|---|---|
| MACD Oscillator | Short/long moving averages of close; position follows their ordering | Hour/day suitable | Candidate |
| Pair trading | Engle-Granger cointegration, standardized residual, long cheap/short rich leg | Daily preferred; hourly possible | Candidate after universe/pair screening |
| Heikin-Ashi | Transform OHLC to smoothed candles and trade trend/reversal states | Hour/day suitable | Candidate |
| London Breakout | Last Tokyo/London-overlap-hour range followed by checks in the opening minutes | Intraday minute execution | Exclude: violates cadence |
| Awesome Oscillator | Short/long averages of median price `(high + low) / 2`, including saucer logic | Hour/day suitable | Candidate/indicator |
| Oil Money | Correlation/causality between oil and petro-currencies | Daily suitable, but FX/commodity thematic | Defer: not equity/options first |
| Dual Thrust | Prior-day OHLC range sets current-session upper/lower breakout thresholds | Hourly entry from daily state | Candidate |
| Parabolic SAR | Recursive SAR/acceleration-factor trend state against close | Hour/day suitable | Candidate |
| Bollinger pattern recognition | Bollinger-band reversals and W-bottom pattern recognition | Hour/day suitable | Candidate |
| RSI pattern recognition | RSI overbought/oversold plus head-and-shoulders/pattern rules | Hour/day suitable | Candidate |
| Monte Carlo project | Simulation/backtest sensitivity analysis | Other: validation technique, not a signal | Use later for robustness |
| Options Straddle | Long call plus long put at the same strike/expiry; payoff/backtest | Daily suitable | Candidate once historical chains are complete |
| Portfolio Optimization | Portfolio construction/optimization | Other: sizing/risk, not a signal | Use in portfolio phase |
| Smart Farmers | Agricultural supply/demand forecasting from alternative data | Other asset/alternative data | Defer |
| VIX Calculator | CBOE-style VIX calculation from two option expiries, strikes, rates, and minutes to expiry | Daily analytics, not itself a strategy | Needed option feature; data gap |
| Wisdom of Crowds | WallStreetBets text/sentiment alternative-data research | Daily possible but source absent locally | Defer: no social feed |
| Shooting Star | OHLC candlestick body/wick pattern, short entry, stop/exit | Hour/day suitable | Candidate but lower priority |

The checked-out source contains runnable top-level files for MACD, pair trading, Heikin-Ashi, London Breakout, Awesome Oscillator, Dual Thrust, Parabolic SAR, Bollinger, RSI, options straddle, shooting star, and the VIX calculator. Several README projects are descriptions or project folders rather than reusable strategy modules. All scripts need reimplementation behind the future engine contract; direct copying would retain deprecated APIs, chained pandas mutation, look-ahead risks, and frictionless portfolio assumptions.

### 3.2 `wilsonfreitas/awesome-quant`: relevant libraries and techniques

This repo offers a curated index, not concrete trading rules. Enumerating every entry would mix hundreds of languages and unrelated products into the implementation plan, so the inventory below enumerates the concrete *relevant library families and named Python choices* found in its current README; HFT-only and non-Python alternatives are tagged rather than proposed.

| Offering found in index | Concrete examples | Classification | Proposed use |
|---|---|---|---|
| Dataframes/numerics | NumPy, pandas, Polars, SciPy | Hour/day suitable | Use DuckDB plus Polars/pandas for batch feature work |
| Technical indicators | `ta`, TA-Lib, `finta`, `talipp`, `streaming_indicators`, `bta-lib`, TuneTA | Hour/day suitable | Prefer a small internal indicator contract; evaluate `ta`/`talipp`, avoid making signals library-specific |
| Backtesting | Backtesting.py, backtrader, bt, QSTrader, Zipline Reloaded, vectorbt, PyBroker, Lean, Lumibot, Qlib, pyqstrat | Hour/day suitable | Evaluate vectorbt/PyBroker for research ideas, but build a thin DuckDB-native engine to preserve point-in-time universe and option semantics |
| Leakage/validation | `purgedcv`, `rulelint`, backtest-bias, QuantStats | Hour/day suitable | Adopt walk-forward/purged validation and bias checks |
| Portfolio/risk | PyPortfolioOpt, Riskfolio-Lib, empyrical, QuantStats, Kelly-Criterion, ffn | Hour/day suitable | Add after signal correctness; use for sizing/reporting, not alpha |
| Factor analysis | Alphalens/alphalens-reloaded, QuantStats/empyrical ecosystem, Qlib | Daily/cross-sectional suitable | Relevant to whole-market ranking and forward-return evaluation |
| Time-series/statistics | statsmodels, ARCH, tsfresh, sktime and related forecasting packages | Hour/day suitable | Statsmodels for cointegration; defer ML feature generation |
| Options pricing/analytics | QuantLib/PyQL, py_vollib, FinancePy, OptionLab, SABR/SVI and stochastic-volatility packages | Daily suitable | Evaluate `py_vollib` or QuantLib for normalized IV/Greeks; OptionLab for payoff validation |
| Market data | Polygon client, yfinance/yahooquery, IB APIs, EDGAR tools, FinanceDatabase | Hour/day suitable | Existing Polygon/Massive, IBKR, yfinance, EDGAR paths already cover much of this |
| Calendars | `exchange_calendars`, `pandas_market_calendars`, bizdays | Hour/day suitable and required | Add one canonical exchange calendar before resampling |
| Order book/order flow | PyLOB, `orderflow`, trade aggregators, tick/HF data clients | HFT/other | Exclude from this program |
| Crypto/execution bots | Freqtrade, OctoBot, exchange-specific bots and bridges | Other market and often high frequency | Exclude |
| Pricing-only/non-strategy tools | Monte Carlo/pricers, yield-curve, mortgage, spreadsheet, visualization libraries | Other | Use only if a later feature explicitly needs them |

Selection criteria for dependencies must include license, maintenance, Python-version compatibility, ability to consume our own arrays/tables, and no hidden dependency on vendor-specific data. The index is a discovery list, not an endorsement; dependency selection requires a separate spike and benchmark.

### 3.3 `Options-Trading-Strategies-in-Python`

Despite its name, this repository contains four signal scripts and a Monte Carlo pricer, rather than a catalog of multi-leg option structures.

| Source file | Concrete logic/data | Cadence classification | Decision |
|---|---|---|---|
| `PCR_strategy.py` | Daily S&P put/call ratio; 20-day mean and volatility bands; trades S&P futures on band crossings | Daily suitable | Adapt to a market/underlying PCR feature; do not preserve futures-only execution |
| `TRIN_strategy.py` | Daily NYSE advancing/declining issues and volumes; log Arms Index; mean/band crossings | Daily suitable | Defer until breadth data is derived from whole-market bars |
| `Turtle Trading.py` | Prior 55-day high/low entries; prior 55-day mean exits; long and short | Daily suitable | High-priority equity strategy |
| `VIX_Strategy.py` | Long S&P future when VIX exceeds a fixed threshold; ±5% exit | Daily suitable | Lower priority; replace fixed threshold with point-in-time regime rules |
| `Monte Carlo Option Pricing/*` | Simulated European call valuation | Other: pricing, not a signal | Validation utility only |

Important source caveats: the PCR and VIX scripts appear to assign columns from mismatched Quandl datasets, use expired embedded credentials/legacy endpoints, and use fixed contract multipliers. They are specifications to test, not code to port.

## 4. Current IBKR_workbench inventory

### 4.1 What exists

There is no `strategy`, `signal`, `backtest`, order simulation, portfolio-accounting, or factor module today. The repository is an ETL/data workbench with DuckDB persistence, RAG/text-to-SQL, a Streamlit dashboard, and medallion-oriented tests.

| Current dataset/table | Actual schema/cadence | Scope and limitations |
|---|---|---|
| `polygon_bars` | `(ticker, ts, timespan, OHLCV, vwap, transactions)`; unique `(ticker, ts, timespan)`; supports day/minute/hour strings | REST can request hour/day, but bulk minute and daily loaders retain only a hard-coded 32-name set |
| `stock_quotes` | IBKR point snapshots: bid/ask/last/OHLC/volume/VWAP | Configured universe; not historical bars |
| `polygon_snapshots` | Delayed stock bid/ask/last/previous close/day volume | Point-in-time inserts; not a reproducible bar history |
| `polygon_option_bars` | Contract identity plus daily/minute OHLCV/VWAP/transactions | REST lists contracts per underlying, defaults/caps at 100 in `main.py`, optional fixed underlying override; no whole-market flat loader |
| `polygon_option_snapshots` | Underlying/expiry/strike/right, day OHLC/volume, OI, IV, delta/gamma/theta/vega | Point-in-time chain snapshot; no uniqueness key/history-completeness guarantee |
| `option_chains` | IBKR expiry/strike/right/exchange, fetched time | Metadata for configured symbols; no contract listing/delisting validity interval |
| `option_quotes` | IBKR bid/ask/last, volume/OI, IV/Greeks, underlying price | Nearest configured expiry cycles and configured tickers only |
| `polygon_tickers` | Symbol/name/market/exchange/type/active/currency/description/current update | Current reference snapshot, not a point-in-time membership/security master |
| `staging_yf_bars` | Daily OHLC, adjusted close, volume, dividends/splits | Defaults to the same 32 validation names |
| `staging_yf_indices` / `_stats` | Daily index OHLCV and derived return/volatility/drawdown statistics | 16 source indices plus two derived spreads; useful regime context |
| `cot_reports` | Weekly futures positioning and open interest | Useful for later futures/macro signals, not first equity/options tranche |
| `edgar_facts`, filings, 13F | Filing dates, periods and financial facts/holdings | Potential daily fundamental features; requires point-in-time normalization before backtests |
| `polygon_trades` | Individual trades | Explicitly out of scope for mid-frequency strategies |
| `output.csv` | Symbol/name reference export | Not a historical market dataset and not a suitable universe authority |

### 4.2 Existing flat-file behavior

- `etl/bulk_load_massive.py` syncs `us_stocks_sip/minute_aggs_v1/<year>` into `data/minute_aggs/<year>`, then DuckDB reads every date file but filters to its hard-coded `TICKERS` set.
- `etl/bulk_load_daily.py` syncs `us_stocks_sip/day_aggs_v1/<year>` into `data/day_aggs/<year>`, then loops CSV rows in Python and applies the same fixed set.
- `etl/extract_polygon.py` is REST-per-ticker for stocks and REST-per-contract for option bars. `main.py` limits option history through `POLYGON_OPTION_BARS_TICKERS`, lookback, and `POLYGON_OPTION_BARS_MAX_CONTRACTS`.
- Paths contain a Windows-specific `AWS_CLI`; credentials/profile, download, normalization, and load checkpoints are coupled.
- Bronze CSV.gz files are mixed conceptually with curated DuckDB tables. There are no manifests, checksums, source dates, schema versions, quarantine, or reproducible silver Parquet assets.
- Flat-file stock data is unadjusted. The current table does not carry adjustment factors, listing validity, or canonical security identifiers, creating split and survivorship hazards.

## 5. Data-to-strategy fit

| Strategy/feature | Required inputs | Present now? | Required addition |
|---|---|---|---|
| MACD/Awesome Oscillator | Complete adjusted close, or high/low, by symbol | Partial | Whole-market bars, corporate actions, liquidity/universe filters |
| Turtle/Donchian | At least 55 completed daily closes/highs/lows | Partial | Same; add ATR/risk sizing later |
| Bollinger/RSI | Adjusted OHLCV and rolling history | Partial | Same; enforce lagged signals and minimum liquidity |
| Heikin-Ashi/Parabolic SAR/Shooting Star | OHLC; optional volume/ATR | Partial | Whole-market complete bars and split adjustment |
| Dual Thrust | Prior-day OHLC plus current completed hourly bars | Partial | Whole-market hourly silver bars and session calendar |
| Pairs | Adjusted synchronized close, sector/industry, liquidity, delisting history | Partial | Point-in-time universe, classifications, scalable pre-screen, borrow assumptions |
| Cross-sectional momentum/factors | Whole-market adjusted returns, volume, reference membership | Partial | Point-in-time security master and daily cross-section |
| Straddle | Point-in-time chain, contract bid/ask or bars, underlying, expiry/strike/right, rates/dividends, multiplier | Partial/insufficient | Whole-market option aggregates, quotes for executable fills, contract reference, rates/dividends |
| PCR | Put/call daily volume and preferably OI, consistently scoped by market/underlying | Partial | Option flat aggregates provide volume; daily OI snapshots/history need a separate feed |
| VIX calculation | Two expiries of bid/ask quotes across strikes, risk-free curve, exact timestamps/calendar | No | Historical option quotes plus Treasury/rate curve and robust contract metadata |
| IV-realized-volatility | Historical IV surface or bid/ask-derived IV, underlying realized vol, rates/dividends | Partial/insufficient | Historical option quote/IV surface feed; aggregates alone cannot reconstruct executable IV reliably |
| TRIN/breadth | Daily advancing/declining issues and their volumes over a valid exchange universe | Derivable after redesign | Point-in-time NYSE membership/classification and whole-market bars |
| EDGAR value/quality | Filing-date-aware standardized facts, prices, shares, corporate actions | Raw facts exist | Point-in-time fact selection, shares/market cap normalization |
| COT | Weekly positioning plus futures continuous series | COT only | Continuous futures bars/roll rules; defer |

“Partial” is intentionally strict: a schema can hold the field, but the current ingestion scope is not sufficient for a whole-market, survivorship-aware backtest.

## 6. Prioritized strategy shortlist

### P0: engine acceptance strategies

| Strategy | Inputs and cadence | Direction | Rough signal | Feeds |
|---|---|---|---|---|
| Cross-sectional equity momentum/trend | Adjusted daily close/volume; daily ranking | Long-only first, then market-neutral long/short | Rank 6–12 month return skipping the most recent month, or rank MACD/trend state; rebalance daily/weekly; liquidity and price filters | Silver stock daily bars; security master; corporate actions. Existing Polygon flat files supply raw OHLCV |
| Turtle/Donchian breakout | 55 completed daily highs/lows and mean/ATR | Long/short; offer long-only mode | Enter above lagged rolling high or below lagged low; exit through lagged mean/opposite channel | Same daily stock data; borrow/shortability later |
| Bollinger/RSI mean reversion | Daily adjusted OHLCV; optional hourly confirmation | Long-only first, then long/short | Enter liquid names at lagged lower-band/oversold condition; exit at mean/neutral RSI; explicit stop and max holding period | Daily/hourly silver bars |

These three exercise cross-sectional selection, rolling state, fills, costs, and portfolio accounting without requiring option-specific infrastructure.

### P1: expanded equity signals

| Strategy | Inputs and cadence | Direction | Rough signal | Feeds |
|---|---|---|---|---|
| Heikin-Ashi + Parabolic SAR confirmation | Completed hourly or daily OHLC | Long/short or long-only | Require agreement between smoothed candle trend and lagged SAR state; volatility-scaled exit | Silver stock hourly/daily |
| Dual Thrust | Prior-day OHLC and completed hourly OHLC | Long/short | Build range from lagged daily OHLC; trade hourly close through session thresholds; flatten or carry by tested rule | Daily and hourly silver stock bars; exchange calendar |
| Pairs/stat-arb | Synchronized adjusted daily close, sector/industry, liquidity | Dollar/beta-neutral long/short | Pre-screen within industry, rolling Engle-Granger test, enter residual z-score extremes, exit near mean; invalidate broken pairs | Stock daily, reference/classification, borrow and costs |
| Shooting Star | Completed hourly/daily OHLC and ATR/volume | Short; optional long-only risk-off overlay | Detect upper-wick/small-body reversal after an uptrend; cover on stop/target/time | Stock daily/hourly |

Pairs is placed after simpler strategies because an all-pairs scan over roughly 11,000 symbols is infeasible and statistically dangerous; sector/liquidity screening and multiple-testing controls are dependencies.

### P2: options and market sentiment

| Strategy | Inputs and cadence | Direction | Rough signal | Feeds |
|---|---|---|---|---|
| Put/call-ratio regime | Daily option call/put volume; OI when available; underlying/index bar | Long/short underlying or portfolio risk overlay | Aggregate by market and underlying; standardize PCR versus lagged history; act on band cross/reversion | New option day-aggregate silver; separate historical OI feed |
| Liquid long straddle research | Daily/hourly option bars, preferably NBBO, underlying, contract master, rates/dividends | Long volatility, defined risk | Select near-ATM same-expiry call/put under point-in-time liquidity/DTE rules; enter only when IV/realized-volatility regime justifies premium | New option flat aggregates; option quotes/NBBO required for credible fills |
| IV versus realized-volatility / volatility risk premium | Historical IV surface/NBBO plus adjusted underlying bars and rates | Defined-risk long or short volatility | Compare standardized IV to forecast realized vol; express through spreads/straddles with exposure limits | Existing snapshots are insufficient; add quote/IV history |
| VIX-style market volatility feature | Full SPX-like option quotes for two expiries, rates and calendar | Regime feature, not initially a trade | Implement/test CBOE-style variance calculation and use as market-risk filter | New quote history and rate curve |

Short-premium naked positions are not in the first release. Multi-leg spreads require atomic-fill modeling, margin, assignment/exercise, multiplier, expiration settlement, and stale/zero-bid handling.

## 7. Whole-market download-pattern redesign

### 7.1 Design principles

1. Date-partition raw and curated assets. The vendor delivers market-wide files per date; per-symbol files create millions of small objects and make daily cross-sectional work expensive.
2. Preserve raw bytes immutably in bronze. Normalize to typed, compressed Parquet in silver. Build research-ready adjusted bars/features in gold.
3. Define “whole market” through a point-in-time security master, not today’s YAML or current-active ticker table.
4. Derive hourly bars from minute aggregates with an exchange calendar. Do not download ticks or fabricate hourly bars from daily data.
5. Make every date independently retryable and auditable through manifests.

### 7.2 Proposed layout

```text
data/
  bronze/massive/
    stocks/day_aggs_v1/year=YYYY/month=MM/YYYY-MM-DD.csv.gz
    stocks/minute_aggs_v1/year=YYYY/month=MM/YYYY-MM-DD.csv.gz
    options/day_aggs_v1/year=YYYY/month=MM/YYYY-MM-DD.csv.gz
    options/minute_aggs_v1/year=YYYY/month=MM/YYYY-MM-DD.csv.gz
    reference/tickers_v3/as_of_date=YYYY-MM-DD/part-*.json.gz
    reference/options_contracts/as_of_date=YYYY-MM-DD/part-*.json.gz
    corporate_actions/{splits,dividends}/as_of_date=YYYY-MM-DD/part-*.json.gz
    manifests/dataset=<name>/year=YYYY/month=MM/manifest.parquet
  silver/
    stock_bars_1d/year=YYYY/month=MM/part-*.parquet
    stock_bars_1h/year=YYYY/month=MM/part-*.parquet
    option_bars_1d/year=YYYY/month=MM/part-*.parquet
    option_bars_1h/year=YYYY/month=MM/part-*.parquet
    securities/as_of_year=YYYY/part-*.parquet
    option_contracts/expiry_year=YYYY/expiry_month=MM/part-*.parquet
    corporate_actions/year=YYYY/part-*.parquet
  gold/
    adjusted_stock_bars_1d/year=YYYY/month=MM/part-*.parquet
    adjusted_stock_bars_1h/year=YYYY/month=MM/part-*.parquet
    daily_universe/as_of_date=YYYY-MM-DD/part-*.parquet
    option_chain_eod/trade_date=YYYY-MM-DD/part-*.parquet
    features/frequency=1d/strategy=<name>/year=YYYY/month=MM/part-*.parquet
    features/frequency=1h/strategy=<name>/year=YYYY/month=MM/part-*.parquet
```

Parquet should use Zstandard compression, typed UTC timestamps, `DATE` trade dates, dictionary-encoded symbols, stable contract/security identifiers, and a target file size around 128–512 MB. Compaction should combine date files within a month without losing a `trade_date` column. Symbol hash buckets may be added only if measured queries require pruning; do not return to one-file-per-symbol.

### 7.3 Stock flat files

- Backfill raw daily files from `s3://flatfiles/us_stocks_sip/day_aggs_v1`; remove the `TICKERS` predicate entirely.
- Backfill raw minute files from `us_stocks_sip/minute_aggs_v1` only for the retention horizon needed to derive hourly signals. Aggregate all regular-session minutes to canonical 60-minute buckets in silver. Define the final partial session bucket and early-close behavior explicitly.
- Keep daily aggregates as authoritative raw daily input instead of recomputing them from minute files; reconciliation tests compare both paths.
- Persist adjusted and unadjusted prices separately. Apply point-in-time split/dividend factors in gold and retain the raw vendor values.
- Build `daily_universe` from listing/delisting/type/exchange validity. Default research eligibility should exclude non-common-stock instruments only through a documented policy, while the raw ingestion still covers the whole file.

### 7.4 Option flat files

- Replace `list_options_contracts` plus one `get_aggs` request per contract with date-level OPRA aggregate files:
  - `s3://flatfiles/us_options_opra/day_aggs_v1` for all-market daily option bars.
  - `s3://flatfiles/us_options_opra/minute_aggs_v1` when hourly option signals are approved; derive hourly in silver.
- Parse the option ticker into a normalized contract key, but also ingest the vendor contract-reference endpoint so roots with adjustments, non-standard deliverables, multipliers, exercise style, and corrected metadata are not inferred from OCC text alone.
- Partition by trade date/month, not underlying or contract. Expose DuckDB views that prune by `underlying`, `expiry`, DTE, moneyness, volume, and open interest.
- Build `option_chain_eod` by joining contract reference, aggregate bars, underlying close, and the best available OI/IV/Greeks snapshot. Preserve missingness: aggregate flat files provide traded OHLCV, not a complete no-trade chain and not executable bid/ask.
- Historical NBBO/quote flat files are a separate, storage-heavy decision. They are required for credible option fills and VIX/IV work but should be scoped to filtered contracts *after* a daily chain candidate pass if licensing permits. No tick strategy is implied.

### 7.5 Incremental refresh and data quality

For each dataset/date:

1. Discover expected trading dates through the exchange calendar.
2. Download to a temporary file, verify gzip/CSV readability, header/schema, nonzero size, and optional checksum/ETag.
3. Atomically promote to bronze and record source path, ETag/checksum, bytes, rows, min/max timestamp, ingestion time, schema version, and status in a manifest.
4. Transform only new/changed bronze partitions into silver Parquet; quarantine schema failures.
5. Rebuild affected hourly, adjustment, universe, and gold feature partitions.
6. Run uniqueness, OHLC invariants, timestamp/session, symbol/contract parse, coverage, and prior-day row-count checks.
7. Re-fetch a rolling correction window (proposed five trading days for aggregates and 30 calendar days for reference/corporate actions), plus an explicit repair command for older dates.

Initial backfill should be resumable by date ranges and bounded concurrency. The daily scheduler should fetch the prior trading day after the vendor availability window, never infer completion from the wall clock alone, and publish a data-ready watermark. Strategies may consume only dates at or before the minimum required-dataset watermark.

### 7.6 Concrete modules to change or add in the implementation phase

| File/module | Planned change |
|---|---|
| `etl/bulk_load_daily.py` | Replace fixed-filter row loop with generic whole-file landing/normalization orchestration; make endpoint/CLI cross-platform |
| `etl/bulk_load_massive.py` | Migrate minute downloading into the same manifest-driven stock pipeline after regression coverage; remove `TICKERS` filtering |
| `etl/extract_polygon.py` | Retain REST for repair/small targeted pulls and snapshots; retire per-contract aggregate backfill as the primary option path |
| `etl/extract_yfinance.py` | Keep as validation/fallback, not whole-market authority; decouple its default list from `bulk_load_massive.TICKERS` |
| `config/tickers.py`, `config/tickers.yaml`, `config/update_tickers.py` | Stop treating a current YAML list as the research universe; keep YAML for live/watchlist execution only |
| `db/database.py` | Add manifest/security-master/corporate-action metadata and DuckDB external Parquet views; migrate timestamp strings to typed view columns |
| `main.py` | Add dataset/frequency/date-range jobs, separate download/normalize/publish stages, and remove watchlist semantics from bulk jobs |
| New `etl/flatfiles/{config,download,manifest,normalize,quality}.py` | Shared, dataset-driven flat-file framework |
| New `etl/flatfiles/{stocks,options}.py` | Asset-specific schemas and transforms |
| New `etl/resample.py` | Calendar-aware minute-to-hour aggregation |
| New `db/migrations/*` | Versioned schema/view migrations rather than only `CREATE IF NOT EXISTS` |
| New bronze/silver/gold tests | Fixtures for complete date files, corrections, contracts, splits, early closes, and watermarks |
| README/dashboard/query helpers | Display coverage/freshness and query Parquet-backed views only after the data contract is stable |

The prior design document said not to touch the minute loader. That was appropriate for its additive daily-loader task; the new whole-market goal supersedes that architectural constraint, but the migration must preserve the old job until parity tests pass.

## 8. Backtest and signal-engine design

Add the engine only after gold daily/hourly bars and point-in-time universes pass coverage gates.

Proposed package boundaries:

```text
research/
  data.py           # point-in-time bars, chains, universe, feature reads
  indicators.py     # pure lag-aware transforms
  strategy.py       # strategy protocol and parameter schema
  signals.py        # immutable signal records
  portfolio.py      # sizing, cash, exposures, constraints
  execution.py      # next-bar fills, spreads, slippage, commissions
  options.py        # contract selection, multi-leg lifecycle, expiry/assignment
  backtest.py       # event clock and orchestration
  metrics.py        # returns, turnover, drawdown, exposure, capacity
  validation.py     # walk-forward/purged splits and leakage checks
```

Core records should include `strategy_id`, `parameter_version`, `as_of_ts`, `effective_ts`, frequency, universe version, instrument/contract ID, target position or score, and data watermark. Signals computed from a bar may not fill at that bar’s close unless an explicit auction model justifies it.

Minimum engine acceptance criteria:

- Point-in-time universe and delisted names; no use of today’s active list in historical periods.
- Corporate-action-aware returns and symbol changes.
- Next-bar/event ordering, warm-up periods, missing bars, halts, and early closes.
- Configurable commissions, spread/slippage, participation limits, borrow availability/fees, and short constraints.
- Portfolio cash, gross/net/sector exposure, turnover, and position limits.
- Reproducible parameter/data versions; walk-forward and purged validation.
- Options lifecycle: multiplier, DTE selection, no-trade/zero-bid handling, multi-leg costs, expiration, exercise/assignment, settlement, and margin approximation.
- Benchmarks, survivorship tests, no-look-ahead tests, and deterministic golden fixtures.

## 9. Phased roadmap

### Phase 0 — decisions and measurements

- Confirm Massive/Polygon stock and OPRA flat-file entitlements, history, correction behavior, and expected storage.
- Choose regular-hours versus extended-hours hourly bars and a canonical calendar.
- Measure one recent month of stock minute, stock daily, option daily, and option minute compressed/uncompressed sizes.
- Define “whole market” raw coverage versus research-eligible common-stock universe.

**Exit:** approved data contract, cost/storage budget, and retention policy.

### Phase 1 — whole-market daily data foundation

- Implement manifest-driven bronze downloads for stock and option daily aggregates.
- Implement silver Parquet schemas, security/contract reference, corporate actions, and DuckDB views.
- Remove fixed ticker filtering from bulk paths.
- Backfill/reconcile daily data and publish coverage watermarks.

**Dependencies:** data entitlement, disk/object storage, security-master policy.  
**Exit:** reproducible whole-market daily stock/option coverage with quality reports.

### Phase 2 — hourly foundation

- Land whole-market stock minute aggregates for the agreed horizon and derive calendar-correct hourly bars.
- Decide whether option hourly signals justify option-minute storage; if yes, use the same date pipeline and candidate-aware gold views.
- Add partition compaction and retention.

**Dependencies:** Phase 1 manifests/reference data and storage benchmark.  
**Exit:** complete, validated 1-hour stock bars; option hourly only if approved.

### Phase 3 — research/backtest MVP

- Implement data, strategy, signal, execution, portfolio, metrics, and validation contracts.
- Use daily momentum, Turtle, and Bollinger/RSI as engine acceptance fixtures.
- Add dashboard/query read-only results only after deterministic tests pass.

**Dependencies:** gold adjusted bars and point-in-time universe.  
**Exit:** bias-aware reproducible daily cross-sectional backtests with costs.

### Phase 4 — equity strategy expansion

- Add Heikin-Ashi/SAR, Dual Thrust, pairs, and shooting-star variants.
- Run walk-forward comparisons and reject variants that fail robustness/capacity gates.

**Dependencies:** hourly bars for Dual Thrust; classification/borrow data for pairs/shorts.

### Phase 5 — options engine and strategies

- Build chain snapshots, contract selection, option fill/lifecycle accounting, and Greeks/IV normalization.
- Add PCR first, then long straddles; add IV-realized-volatility only after quote/IV history is credible.
- Treat VIX calculation as a validated feature, not an initial tradable strategy.

**Dependencies:** option reference, historical quotes/OI/IV decision, rates/dividends, margin/lifecycle model.

### Phase 6 — later features

- Point-in-time EDGAR factors, TRIN breadth, portfolio optimization, COT/futures, alternative data, and live/paper signal publication.
- Tick/HFT strategies remain explicitly out of scope.

## 10. Open questions requiring user decisions

1. Does “whole market” mean raw ingestion of every record plus a common-stock research universe, or should ETFs, ADRs, preferreds, warrants, OTC names, and inactive/delisted securities also be eligible?
2. What Massive/Polygon stock and OPRA flat-file plans and historical depths are licensed? Are option quote flat files included?
3. How much local/object storage is available, and how many years of minute data should be retained to derive hourly bars?
4. Should hourly equity bars use regular trading hours only? How should the final 30-minute regular-session interval be represented?
5. Are signals evaluated at close and filled next open, or is a close-auction execution model desired?
6. Is the first portfolio long-only, long/short with borrow data, or both?
7. For options, are long-premium strategies sufficient initially, or must spreads/short premium and realistic margin/assignment ship in the first options milestone?
8. Is historical open interest/IV/NBBO available from another source, or should the plan budget for a new licensed feed?
9. Should Parquet remain local with DuckDB, or should bronze/silver/gold be object-store compatible from day one?
10. What minimum liquidity, price, history, and capacity rules define an eligible instrument?

## 11. Explicit non-goals

- No production strategy code in this planning change.
- No tick, order-book, market-making, latency-arbitrage, or sub-hour execution strategy.
- No fixed ticker list as the historical research universe.
- No claim that aggregate-bar option backtests have executable fills without quotes/spreads.
- No direct port of reference scripts without independent tests for correctness, bias, costs, and data availability.
