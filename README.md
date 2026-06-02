# IBKR ETL Pipeline

Python ETL that extracts **stock quotes**, **option chains**, and **option Greeks/pricing** from Interactive Brokers TWS API and stores them in a local SQLite database.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.10+ | |
| TWS or IB Gateway | Running locally |
| API access enabled | TWS → Edit → Global Configuration → API → Settings → Enable ActiveX and Socket Clients |
| Paper or Live account | Port 7497 = paper, 7496 = live |

---

## Setup

```bash
# 1. Clone / copy the project
cd ibkr_etl

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure
cp .env.example .env
# Edit .env — set your tickers, port, DB path, etc.
```

---

## Configuration (`.env`)

| Variable | Default | Description |
|---|---|---|
| `TWS_HOST` | `127.0.0.1` | TWS host |
| `TWS_PORT` | `7497` | 7497 paper / 7496 live |
| `TWS_CLIENT_ID` | `1` | Unique client ID |
| `DB_PATH` | `./data/ibkr.db` | SQLite file location |
| `STOCK_TICKERS` | `AAPL,MSFT,SPY` | Comma-separated tickers |
| `OPTIONS_EXPIRY_CYCLES` | `2` | How many nearest expiries to quote |
| `POLL_INTERVAL_SECONDS` | `60` | Interval for scheduled mode |
| `LOG_LEVEL` | `INFO` | DEBUG / INFO / WARNING / ERROR |

---

## Usage

```bash
# Run all jobs once (stocks + options)
python main.py

# Run only stock quotes
python main.py --job stocks

# Refresh option chain metadata only
python main.py --job chain

# Run option quotes only (uses cached chain from DB)
python main.py --job options

# First-time: refresh chain, then run everything
python main.py --refresh-chain

# Continuous mode (polls every POLL_INTERVAL_SECONDS)
python main.py --schedule

# Continuous + refresh chain on first run
python main.py --schedule --refresh-chain
```

---

## Database Schema

### `stock_quotes`
Stores a row every time stock data is polled.

| Column | Type | Description |
|---|---|---|
| ticker | TEXT | e.g. `AAPL` |
| ts | TEXT | UTC ISO-8601 timestamp |
| bid / ask / last | REAL | Prices |
| close | REAL | Prior close |
| volume | INTEGER | Traded volume |
| open / high / low | REAL | Day OHLC |

### `option_quotes`
| Column | Type | Description |
|---|---|---|
| ticker | TEXT | Underlying symbol |
| expiry | TEXT | `YYYYMMDD` |
| strike | REAL | Strike price |
| right | TEXT | `C` or `P` |
| bid / ask / last | REAL | Prices |
| volume | INTEGER | |
| open_interest | INTEGER | |
| implied_vol | REAL | |
| delta / gamma / theta / vega | REAL | Greeks |

### `option_chains`
Metadata: all available expiry/strike/right combinations for each ticker.

### `etl_runs`
Audit log of every ETL job execution.

---

## Querying the Data

```python
from dotenv import load_dotenv
load_dotenv()

from query import latest_stock_quotes, latest_option_quotes, stock_history

# Latest price for all tickers
df = latest_stock_quotes()
print(df[["ticker","last","volume","ts"]])

# AAPL options for nearest expiry
opts = latest_option_quotes("AAPL")
calls = opts[opts.right == "C"]
print(calls[["strike","bid","ask","delta","implied_vol"]])

# AAPL price over last 4 hours
hist = stock_history("AAPL", hours=4)
print(hist)
```

Or run the CLI summary:
```bash
python query.py
```

---

## Project Structure

```
ibkr_etl/
├── main.py                  # Entry point & scheduler
├── query.py                 # Query helpers + CLI summary
├── requirements.txt
├── .env.example
├── db/
│   └── database.py          # SQLite schema + connection
├── etl/
│   ├── ibkr_client.py       # TWS API wrapper (EWrapper + EClient)
│   ├── extract_stocks.py    # Stock snapshot ETL
│   └── extract_options.py   # Option chain + quote ETL
├── data/                    # SQLite DB lives here (auto-created)
└── logs/                    # Daily rotating log files
```

---

## Tips

- **First run**: always use `--refresh-chain` to populate option chain metadata before quoting.
- **Rate limits**: IBKR limits concurrent snapshot requests (~50). The ETL batches automatically.
- **Market hours**: Snapshots outside market hours return stale/delayed data. Schedule during RTH for live prices.
- **Multiple tickers**: Large option chains (SPY, QQQ) have thousands of strikes — narrow `OPTIONS_EXPIRY_CYCLES` to 1-2 to keep runtime reasonable.
