# IBKR Workbench

A full-stack quantitative research platform — live market data from **Interactive Brokers**, historical **OHLCV + options bars from Polygon.io**, **SEC EDGAR financials**, and an **AI chat interface** to query it all in plain English.

---

## What it does

| Layer | What's built |
|---|---|
| **Data ingestion** | ETL pipeline pulling from IBKR TWS, Polygon.io, SEC EDGAR, Finviz, and **CFTC COT** |
| **Storage** | DuckDB (`ibkr.duckdb`) — 14 tables covering equities, options, forex, futures, indices, and COT |
| **Vector search** | DuckDB VSS (`vectors.duckdb`) — HNSW index on 384-dim ticker embeddings |
| **Dashboard** | Streamlit + Plotly — 8 pages including candlestick charts, options chain, and COT positioning |
| **AI chat** | Text-to-SQL (**DeepSeek / Xiaomi MiMo**) + RAG over EDGAR financials |
| **Deployment** | Docker Compose — separate dashboard and ETL containers |

---

## Architecture

```
Data Sources            ETL Pipeline              Storage
────────────            ────────────              ───────
IBKR TWS API  ────────► extract_stocks.py  ─────► ibkr.duckdb
               ────────► extract_options.py ────►   stock_quotes
Polygon.io    ────────► extract_polygon.py ─────►   option_quotes / option_chains
               ────────►   bars, snapshots  ─────►   polygon_bars
               ────────►   options bars     ─────►   polygon_option_bars
               ────────►   option snapshots ─────►   polygon_option_snapshots
               ────────►   reference        ─────►   polygon_tickers / snapshots
SEC EDGAR     ────────► extract_edgar.py   ─────►   edgar_filings / edgar_facts
CFTC COT      ────────► extract_cot.py     ─────►   cot_reports
Finviz        ────────► update_tickers.py  ─────► config/tickers.yaml (11k+ tickers)
                                                 ┌────────────────────────────────┐
                                                 │  vectors.duckdb                │
embed_tickers.py ──────────────────────────────► │  ticker_embeddings (HNSW)      │
                                                 └────────────────────────────────┘

Interfaces
──────────
ibkr.duckdb ──► Streamlit Dashboard (8 pages, Plotly charts)
            ──► chat_engine.py  (Text-to-SQL via DeepSeek / Xiaomi MiMo)
vectors.duckdb ► rag_engine.py  (LangChain RAG — EDGAR + descriptions)
query.py    ──► Python API (latest_stock_quotes, stock_history, etl_run_log…)
```

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.11+ | |
| TWS or IB Gateway | For live IBKR data only — all other jobs work without it |
| Polygon.io API key | Free tier: 5 req/min; Starter $29/mo for full speed |
| DeepSeek / Xiaomi key | For the AI chat interface |
| Docker (optional) | For containerised deployment |

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure
cp .env.example .env
# Fill in: POLYGON_API_KEY, DEEPSEEK_API_KEY (or set CHAT_PROVIDER=mimo for Xiaomi)

# 3. Fetch 11,000+ US tickers from Finviz
python -m config.update_tickers

# 4. Download full history from Polygon (stocks + options)
python main.py --job polygon-ref       # ticker metadata
python main.py --job polygon-bars      # OHLCV + VWAP daily bars (max history)
python main.py --job polygon-option-bars  # historical options bars

# 5. Pull SEC EDGAR financials
python main.py --job edgar-filings
python main.py --job edgar-facts

# 6. Build vector index for AI chat
python main.py --job embed-tickers

# 7. Launch the dashboard
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
| `POLYGON_BARS_LOOKBACK` | `9500` | Days of history (~26 years max) |
| `POLYGON_RATE_DELAY` | `13` | Seconds between calls (free=13, paid=0.1) |
| `POLYGON_OPTION_BARS_TICKERS` | 12 liquid names | Comma-separated underlyings for options history |
| `POLYGON_OPTION_BARS_MAX_CONTRACTS` | `250` | Max contracts per underlying |

### AI Chat
| Variable | Default | Description |
|---|---|---|
| `CHAT_PROVIDER` | `deepseek` | `deepseek` / `mimo` |
| `CHAT_MODEL` | *(provider default)* | Override model name |
| `DEEPSEEK_API_KEY` | | platform.deepseek.com |
| `OLLAMA_BASE_URL` | `http://localhost:11434/v1` | Local Ollama endpoint for Xiaomi MiMo |
| `OLLAMA_MODEL` | `xiaomi/MiMo-7B-RL` | Xiaomi model pulled via `ollama pull` |

### Storage
| Variable | Default | Description |
|---|---|---|
| `DB_PATH` | `./data/ibkr.duckdb` | Main DuckDB database |
| `DUCKDB_PATH` | `./data/vectors.duckdb` | Vector store |
| `TICKERS_YAML` | `config/tickers.yaml` | Ticker universe |

---

## ETL Jobs

```bash
# ── Polygon.io (no TWS needed) ────────────────────────────────────────────────
python main.py --job polygon-ref          # ticker metadata (name, exchange, description)
python main.py --job polygon-bars         # OHLCV + VWAP daily bars (full history)
python main.py --job polygon-quotes       # delayed stock snapshots (bid/ask/last)
python main.py --job polygon-options      # options chain snapshots + Greeks
python main.py --job polygon-option-bars  # historical OHLCV bars per options contract
python main.py --job polygon              # all polygon jobs
python main.py --job polygon-daily-flat   # Polygon day_aggs_v1 S3 flat-file load (additive — minute loader untouched)

# ── yfinance (no API key needed — staging area) ──────────────────────────────
python main.py --job yf-bars              # daily bars for 32 semi/Mag7 tickers → staging_yf_bars
python main.py --job yf-indices           # daily bars + stats for 18 major indices → staging_yf_indices(+stats)

# ── SEC EDGAR (no API key needed) ─────────────────────────────────────────────
python main.py --job edgar-filings        # 10-K / 10-Q / 8-K filing history
python main.py --job edgar-facts          # XBRL financials (revenue, EPS, assets…)

# ── CFTC COT (no API key needed) ──────────────────────────────────────────────
python main.py --job cot                  # Commitments of Traders (Legacy Futures)

# ── AI / Vector search ────────────────────────────────────────────────────────
python main.py --job embed-tickers        # embed ticker descriptions → HNSW index

# ── IBKR live data (requires TWS running) ────────────────────────────────────
python main.py --job stocks               # live stock quotes
python main.py --job options              # live option quotes
python main.py --job chain                # refresh option chain metadata
python main.py --schedule                 # continuous mode

# ── Ticker universe ───────────────────────────────────────────────────────────
python -m config.update_tickers           # fetch all ~11,000 US stocks from Finviz
python -m config.update_tickers --sectors Technology Healthcare
python -m config.update_tickers --dry-run
```

---

## Dashboard

```bash
streamlit run dashboard/app.py
```

| Page | Description |
|---|---|
| 💬 **Chat** | Natural language queries — SQL mode (Text-to-SQL) or RAG mode (knowledge base) |
| 📊 **Stock Quotes** | Live IBKR prices, bid-ask spreads, volume |
| 📉 **Price History** | Candlestick / OHLC / line chart with bid-ask band and volume |
| 📦 **Polygon OHLCV** | Full-history daily bars with VWAP overlay and period return stats |
| 🔗 **Options Chain** | IV smile, Greeks heatmap, OI/volume charts, full chain table |
| 💸 **Cost Calculator** | Round-trip slippage model — spread + IBKR commission + market impact |
| 🩺 **ETL Health** | Job run log, row counts per table, data freshness per ticker (including COT) |
| ℹ️ **About** | Platform overview, data source status, quick reference |

---

## AI Chat

Two modes available on the Chat page:

**SQL mode** — converts your question to DuckDB SQL, executes it, and summarises the result:
> *"Show AAPL closing prices for the last 30 days"*
> *"Which 10 tickers had the highest average volume last month?"*
> *"What was NVDA's revenue for the last 4 quarters?"*

**RAG mode** — searches the vector index and EDGAR facts, then answers from context:
> *"What does Nvidia actually do as a business?"*
> *"Compare Apple and Microsoft's balance sheets"*
> *"Which companies in the semiconductor sector have the most cash?"*

Switch providers with one line in `.env`:
```
CHAT_PROVIDER=deepseek   # or: mimo
```

---

## Asset Coverage

| Asset class | Tickers | Source |
|---|---|---|
| US equities | ~11,200 (NYSE + NASDAQ + AMEX) | Finviz |
| Forex majors + minors | 17 pairs (EUR/USD, GBP/USD…) | IBKR IDEALPRO |
| Equity index futures | ES, NQ, RTY, YM + micro | CME |
| Energy futures | CL, NG, RB, HO, BZ | NYMEX |
| Metals futures | GC, SI, HG, PL, PA | COMEX |
| Rate futures | ZB, ZN, ZF, ZT | CBOT |
| Agricultural futures | ZC, ZS, ZW + 5 more | CBOT |
| FX futures | 6E, 6B, 6J, 6C, 6A, 6S, 6N | CME |
| Crypto futures | BTC, ETH, MBT, MET | CME |
| Cash indices | SPX, VIX, NDX, RUT, DJX + global | CBOE |
| **COT Positioning** | All major futures above (Legacy) | CFTC |

---

## Database Schema

All data in `data/ibkr.duckdb` (14 tables):

| Table | Rows (approx) | Description |
|---|---|---|
| `stock_quotes` | live | IBKR snapshots — bid/ask/last/OHLCV/VWAP |
| `option_quotes` | live | IBKR option quotes — bid/ask/Greeks/IV/OI |
| `option_chains` | live | Chain metadata (expiry × strike × right) |
| `polygon_bars` | ~5,800/ticker | Daily OHLCV + VWAP, full history |
| `polygon_option_bars` | varies | Historical OHLCV bars per contract |
| `polygon_option_snapshots` | point-in-time | Options chain snapshots with Greeks |
| `polygon_snapshots` | point-in-time | Stock delayed snapshots |
| `polygon_tickers` | ~11k | Reference — name, exchange, description |
| `edgar_filings` | varies | 10-K / 10-Q / 8-K filing history |
| `edgar_facts` | varies | XBRL facts — revenue, EPS, assets, equity |
| `cot_reports` | weekly | CFTC Commitments of Traders (Legacy Futures Only) |
| `staging_yf_bars` | ~5,000/ticker | yfinance daily OHLC+adj for the 32 validation tickers — for cross-checking polygon_bars |
| `staging_yf_indices` | ~6,500/index | yfinance daily bars for 18 major indices (incl. 2 derived spreads) |
| `staging_yf_index_stats` | same as indices | Returns/vol/drawdown + 1σ–4σ bands per index per day, rebuilt each run |
| `ticker_embeddings` | ~11k | 384-dim sentence embeddings (HNSW) |
| `edgar_embeddings` | optional | EDGAR filing text embeddings |
| `etl_runs` | grows | Audit log of every ETL job |

Vector store in `data/vectors.duckdb`.

---

## Docker

```bash
# Build and start both services
docker compose up --build

# Dashboard only (read-only)
docker compose up dashboard

# ETL only (scheduled)
docker compose up etl
```

- **dashboard** → `http://localhost:8501`
- **etl** → runs `polygon-bars` on `POLL_INTERVAL_SECONDS` schedule

Both containers mount `./data/` as a shared volume.

---

## Python API

```python
import duckdb
conn = duckdb.connect("data/ibkr.duckdb", read_only=True)

# Full price history for one ticker
conn.execute("""
    SELECT ts, open, high, low, close, volume, vwap
    FROM polygon_bars WHERE ticker = 'AAPL' AND timespan = 'day'
    ORDER BY ts
""").df()

# Historical options bars
conn.execute("""
    SELECT option_ticker, ts, open, high, low, close, volume, vwap
    FROM polygon_option_bars
    WHERE underlying = 'SPY' AND right = 'call' AND expiry >= '2024-01-01'
    ORDER BY option_ticker, ts
""").df()

# Latest EDGAR revenue
conn.execute("""
    SELECT ticker, period_end, value AS revenue
    FROM edgar_facts
    WHERE concept = 'Revenues' AND form_type = '10-K'
    ORDER BY ticker, period_end DESC
""").df()
```

Query helpers:
```python
from query import latest_stock_quotes, stock_history, latest_option_quotes, etl_run_log
from etl.embed_tickers import search_similar_tickers

search_similar_tickers("semiconductor AI chip manufacturer", top_k=10)
```

---

## Project Structure

```
IBKR_workbench/
├── main.py                       # Entry point — all ETL jobs + scheduler
├── query.py                      # Query helpers + CLI summary
├── rag_engine.py                 # LangChain RAG pipeline
├── requirements.txt
├── Dockerfile.dashboard          # Streamlit container
├── Dockerfile.etl                # ETL container
├── docker-compose.yml
│
├── config/
│   ├── tickers.yaml              # 11k+ tickers across all asset classes
│   ├── tickers.py                # Loader — get_all_tickers()
│   └── update_tickers.py         # Finviz scraper with checkpoint/resume
│
├── db/
│   ├── database.py               # DuckDB schema + connection (ibkr.duckdb)
│   └── vector_store.py           # VSS setup reference (now in database.py)
│
├── etl/
│   ├── ibkr_client.py            # TWS API wrapper — EWrapper + EClient
│   ├── extract_stocks.py         # IBKR stock snapshot ETL
│   ├── extract_options.py        # IBKR option chain + quote ETL
│   ├── polygon_client.py         # Polygon REST client factory
│   ├── extract_polygon.py        # Polygon bars / snapshots / options / reference
│   ├── extract_edgar.py          # SEC EDGAR filings + XBRL facts
│   ├── embed_tickers.py          # Sentence-transformer embeddings → vector store
│   ├── chat_engine.py            # Text-to-SQL with read-only SQL validation
│   └── slippage.py               # Transaction cost model
│
├── dashboard/
│   └── app.py                    # Streamlit + Plotly (8 pages)
│
├── data/                         # Auto-created — DuckDB files live here
└── logs/                         # Daily rotating ETL logs
```

---

## Tips

- **Paid Polygon plan** — set `POLYGON_RATE_DELAY=0.1` to cut full-history download from days to ~40 minutes
- **First IBKR run** — always pass `--refresh-chain` to populate option chain metadata first
- **Options bars scale** — each contract = 1 API call; use `POLYGON_OPTION_BARS_TICKERS` to limit scope
- **DuckDB concurrency** — dashboard connects `read_only=True`; only the ETL process writes
- **EDGAR rate limit** — SEC allows 10 req/s; the ETL sleeps 0.12s automatically
- **MiMo locally** — `ollama pull xiaomi/MiMo-7B-RL` then set `CHAT_PROVIDER=mimo` for free local AI
