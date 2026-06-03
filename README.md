# IBKR Workbench

A multi-source market data platform that pulls **live quotes from Interactive Brokers**, **historical OHLCV from Polygon.io**, **SEC EDGAR financials**, and **Finviz ticker universe** — storing everything in **DuckDB** with a **vector search index** and a **Streamlit dashboard**.

---

## Architecture

```
Data Sources                  Storage                  Interfaces
────────────                  ───────                  ──────────
IBKR TWS API  ──────────────► ibkr.duckdb ──────────► Streamlit Dashboard
Polygon.io    ──────────────►   (11 tables)            (6 pages, Plotly charts)
SEC EDGAR     ──────────────►
Finviz        ──► tickers.yaml  vectors.duckdb ──────► Vector search API
                               (HNSW index,            search_similar_tickers()
                                384-dim embeddings)
```

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.11+ | |
| TWS or IB Gateway | For IBKR live data only |
| Polygon.io API key | Free tier works (rate-limited); Starter $29/mo for full speed |
| Docker (optional) | For containerised deployment |

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure
cp .env.example .env
# Fill in POLYGON_API_KEY (and TWS settings if using IBKR)

# 3. Fetch ticker universe (~7,000 US stocks from Finviz)
python -m config.update_tickers

# 4. Download 2 years of daily OHLCV from Polygon
python main.py --job polygon-bars

# 5. Launch the dashboard
streamlit run dashboard/app.py
```

Or with Docker:
```bash
docker compose up --build
# Dashboard → http://localhost:8501
```

---

## Configuration (`.env`)

### IBKR
| Variable | Default | Description |
|---|---|---|
| `TWS_HOST` | `127.0.0.1` | TWS/Gateway host |
| `TWS_PORT` | `7497` | 7497 = paper, 7496 = live, 4002 = Gateway |
| `TWS_CLIENT_ID` | `1` | Unique client ID |
| `OPTIONS_EXPIRY_CYCLES` | `2` | Nearest N expiries to quote |

### Polygon.io
| Variable | Default | Description |
|---|---|---|
| `POLYGON_API_KEY` | *(required)* | From polygon.io/dashboard/api-keys |
| `POLYGON_BARS_TIMESPAN` | `day` | `second` / `minute` / `hour` / `day` |
| `POLYGON_BARS_LOOKBACK` | `730` | Days of history to fetch |
| `POLYGON_RATE_DELAY` | `13` | Seconds between calls (free=13, paid=0.1) |

### Storage
| Variable | Default | Description |
|---|---|---|
| `DB_PATH` | `./data/ibkr.duckdb` | Main DuckDB database |
| `DUCKDB_PATH` | `./data/vectors.duckdb` | Vector store (embeddings) |
| `TICKERS_YAML` | `config/tickers.yaml` | Ticker universe config |

### General
| Variable | Default | Description |
|---|---|---|
| `POLL_INTERVAL_SECONDS` | `60` | Interval for `--schedule` mode |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |

---

## ETL Jobs

```bash
# ── IBKR (requires TWS running) ───────────────────────────────────────────────
python main.py                           # stocks + options (run once)
python main.py --job stocks              # live stock quotes only
python main.py --job options             # live option quotes (uses cached chain)
python main.py --job chain               # refresh option chain metadata
python main.py --schedule --refresh-chain  # continuous mode

# ── Polygon.io (no TWS needed) ────────────────────────────────────────────────
python main.py --job polygon-ref         # ticker metadata (name, exchange, description)
python main.py --job polygon-bars        # OHLCV + VWAP daily bars (2yr default)
python main.py --job polygon-quotes      # delayed stock snapshots
python main.py --job polygon-options     # options chain snapshots + Greeks
python main.py --job polygon             # all 4 polygon jobs

# ── SEC EDGAR (no API key needed) ─────────────────────────────────────────────
python main.py --job edgar-filings       # 10-K / 10-Q / 8-K filing history
python main.py --job edgar-facts         # XBRL financials (revenue, EPS, assets …)

# ── Vector Search ─────────────────────────────────────────────────────────────
python main.py --job embed-tickers       # embed ticker descriptions → HNSW index

# ── Ticker Universe ───────────────────────────────────────────────────────────
python -m config.update_tickers          # fetch all ~7,000 US stocks from Finviz
python -m config.update_tickers --sectors Technology Healthcare  # specific sectors
python -m config.update_tickers --dry-run   # preview counts, don't write
```

---

## Database Schema

All data lives in `data/ibkr.duckdb`.

### IBKR Tables

| Table | Description |
|---|---|
| `stock_quotes` | Live IBKR stock snapshots — bid/ask/last/OHLCV/VWAP |
| `option_quotes` | Live IBKR option quotes — bid/ask/Greeks/IV/OI |
| `option_chains` | Option chain metadata (expiry × strike × right) |
| `etl_runs` | Audit log of every ETL job |

### Polygon Tables

| Table | Description |
|---|---|
| `polygon_bars` | Daily (or intraday) OHLCV + VWAP bars |
| `polygon_snapshots` | Delayed stock snapshots — bid/ask/last |
| `polygon_option_snapshots` | Options chain snapshots with Greeks |
| `polygon_tickers` | Reference data — name, exchange, description |

### EDGAR Tables

| Table | Description |
|---|---|
| `edgar_filings` | 10-K / 10-Q / 8-K filing metadata |
| `edgar_facts` | XBRL financial facts — revenue, net income, EPS, assets, equity, cash |

### Vector Store (`data/vectors.duckdb`)

| Table | Description |
|---|---|
| `ticker_embeddings` | 384-dim sentence embeddings of ticker descriptions (HNSW index) |

---

## Streamlit Dashboard

```bash
streamlit run dashboard/app.py
```

| Page | Description |
|---|---|
| 📊 Stock Quotes | Live IBKR prices, spread comparison, volume |
| 📉 Price History | Candlestick / OHLC / line chart with bid-ask band |
| 📦 Polygon OHLCV | 2-year daily bars with VWAP, period return stats |
| 🔗 Options Chain | IV smile, Greeks heatmap, OI/volume, chain table |
| 💸 Cost Calculator | Round-trip slippage model (spread + commission + market impact) |
| 🩺 ETL Health | Row counts, run log timeline, data freshness per ticker |

---

## Docker

```bash
# Build and start everything
docker compose up --build

# Dashboard only (read-only, no ETL)
docker compose up dashboard

# ETL only (headless, runs on schedule)
docker compose up etl
```

Services:
- **dashboard** — Streamlit on `http://localhost:8501`
- **etl** — runs `polygon-bars` on `POLL_INTERVAL_SECONDS` schedule

Both share `./data/` as a volume mount so the DuckDB files persist and are accessible from either container.

---

## Querying Programmatically

```python
import duckdb
conn = duckdb.connect("data/ibkr.duckdb", read_only=True)

# 2 years of AAPL daily bars
conn.execute("""
    SELECT ts, open, high, low, close, volume, vwap
    FROM polygon_bars WHERE ticker = 'AAPL' AND timespan = 'day'
    ORDER BY ts
""").df()

# Latest IBKR quote per ticker
conn.execute("""
    SELECT ticker, last, bid, ask, volume
    FROM stock_quotes
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ts DESC) = 1
""").df()

# Revenue history from EDGAR
conn.execute("""
    SELECT ticker, period_end, value AS revenue
    FROM edgar_facts
    WHERE concept = 'Revenues' AND form_type = '10-K'
    ORDER BY ticker, period_end
""").df()
```

Or use the query helpers:
```python
from query import latest_stock_quotes, stock_history, latest_option_quotes, etl_run_log

latest_stock_quotes()           # latest price for every ticker
stock_history("AAPL", hours=24) # AAPL last 24h
latest_option_quotes("AAPL")    # all AAPL options (latest snapshot)
etl_run_log(20)                 # last 20 ETL runs
```

Semantic ticker search:
```python
from etl.embed_tickers import search_similar_tickers
search_similar_tickers("semiconductor AI chip manufacturer", top_k=10)
```

---

## Ticker Universe

Tickers are defined in `config/tickers.yaml`, grouped by industry. Run `config/update_tickers.py` to repopulate from Finviz (all US exchanges, ~7,000 stocks):

```bash
python -m config.update_tickers
```

The file is also hand-editable — add or remove any ticker and all ETL jobs pick up the change automatically.

---

## Project Structure

```
IBKR_workbench/
├── main.py                    # Entry point — all ETL jobs + scheduler
├── query.py                   # Query helpers + CLI summary
├── requirements.txt
├── .env.example
├── Dockerfile
├── docker-compose.yml
│
├── config/
│   ├── tickers.yaml           # Ticker universe (grouped by industry)
│   ├── tickers.py             # Loader — get_all_tickers(), get_expiry_cycles()
│   └── update_tickers.py      # Finviz scraper — populates tickers.yaml
│
├── db/
│   ├── database.py            # DuckDB schema + connection (ibkr.duckdb)
│   └── vector_store.py        # DuckDB VSS setup (vectors.duckdb, HNSW index)
│
├── etl/
│   ├── ibkr_client.py         # TWS API wrapper — EWrapper + EClient
│   ├── extract_stocks.py      # IBKR stock snapshot ETL
│   ├── extract_options.py     # IBKR option chain + quote ETL
│   ├── polygon_client.py      # Polygon REST client factory
│   ├── extract_polygon.py     # Polygon bars / snapshots / options / reference ETL
│   ├── extract_edgar.py       # SEC EDGAR filings + XBRL facts ETL
│   └── embed_tickers.py       # Sentence-transformer embeddings → vector store
│
├── dashboard/
│   └── app.py                 # Streamlit + Plotly dashboard (6 pages)
│
├── data/                      # Auto-created — DuckDB files live here
└── logs/                      # Daily rotating ETL logs
```

---

## Tips

- **Free Polygon tier** — 5 req/min (13s delay). Set `POLYGON_RATE_DELAY=0.1` on paid plans.
- **First IBKR run** — always pass `--refresh-chain` to populate option chain metadata before quoting options.
- **Large chains** — SPY/QQQ have thousands of strikes; set `expiry_cycles: 1` in `tickers.yaml` `options_config` to limit scope.
- **DuckDB concurrency** — the dashboard connects `read_only=True`; only the ETL process writes.
- **EDGAR rate limit** — SEC allows 10 req/s; the ETL sleeps 0.12s between requests automatically.
