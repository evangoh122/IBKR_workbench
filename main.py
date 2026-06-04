"""
main.py
Entry point for the IBKR + Polygon ETL pipeline.

Usage:
    python main.py                       # run once, all IBKR jobs
    python main.py --job stocks          # only IBKR stock quotes
    python main.py --job options         # only IBKR option quotes
    python main.py --job chain           # only refresh IBKR option chain metadata
    python main.py --job polygon         # all polygon.io jobs (bars, snapshots, options, reference)
    python main.py --job polygon-bars    # polygon OHLCV bars only
    python main.py --job polygon-quotes  # polygon stock snapshots only
    python main.py --job polygon-options # polygon options chain only
    python main.py --job polygon-ref     # polygon ticker reference only
    python main.py --job polygon-ticks   # polygon trade ticks only (requires START_DATE/END_DATE)
    python main.py --job polygon-semis   # day bars + ticks for group-filtered tickers
    python main.py --job embed-tickers   # embed polygon descriptions → DuckDB vector store
    python main.py --job edgar-filings   # EDGAR filing history (10-K, 10-Q, 8-K)
    python main.py --job edgar-facts     # EDGAR XBRL financial facts
    python main.py --job cot             # CFTC COT reports
    python main.py --schedule            # continuous mode, respects POLL_INTERVAL_SECONDS

Environment variables for group filtering and tick data:
    POLYGON_GROUPS=semiconductors,semiconductor_equipment_and_materials
    START_DATE=2021-01-01
    END_DATE=2026-01-01
    POLYGON_TICK_MAX_PER_TICKER=10000000
"""
import argparse
import os
import sys
import time
import warnings
from functools import wraps

# Suppress specific Python 3.14 / dependency warnings
warnings.filterwarnings("ignore", category=UserWarning, module="langchain_core")
warnings.filterwarnings("ignore", message=".*urllib3.*match a supported version")

import schedule
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
TWS_HOST      = os.getenv("TWS_HOST",     "127.0.0.1")
TWS_PORT      = int(os.getenv("TWS_PORT", "4001"))
TWS_CLIENT    = int(os.getenv("TWS_CLIENT_ID", "1"))
from config.tickers import get_all_tickers, get_all_ticker_symbols, get_tickers_by_groups
TICKERS       = get_all_tickers()          # List of dicts for IBKR jobs
TICKER_SYMBOLS = get_all_ticker_symbols()  # List of strings for REST APIs
POLL_SECS     = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
LOG_LEVEL     = os.getenv("LOG_LEVEL", "INFO")
EXPIRY_CYCLES = int(os.getenv("OPTIONS_EXPIRY_CYCLES", "2"))

POLYGON_TIMESPAN              = os.getenv("POLYGON_BARS_TIMESPAN", "day")
POLYGON_LOOKBACK              = int(os.getenv("POLYGON_BARS_LOOKBACK", "1825"))       # 5 years — bars only
POLYGON_OPT_LOOKBACK          = int(os.getenv("POLYGON_OPTION_BARS_LOOKBACK", "730")) # 2 years — options only
POLYGON_OPT_MAX_CONTRACTS     = int(os.getenv("POLYGON_OPTION_BARS_MAX_CONTRACTS", "1000"))
POLYGON_OPTIONS_MAX_CONTRACTS = int(os.getenv("POLYGON_OPTIONS_MAX_CONTRACTS", "2000"))
POLYGON_START_DATE            = os.getenv("START_DATE", "") or None
POLYGON_END_DATE              = os.getenv("END_DATE",   "") or None
POLYGON_TICK_MAX              = int(os.getenv("POLYGON_TICK_MAX_PER_TICKER", "10000000"))

# Group filtering — if set, only include tickers from these groups
_polygon_groups = os.getenv("POLYGON_GROUPS", "")
if _polygon_groups:
    _group_names = [g.strip() for g in _polygon_groups.split(",") if g.strip()]
    TICKERS = get_tickers_by_groups(_group_names)
    TICKER_SYMBOLS = [t["symbol"] for t in TICKERS]
    logger.info(f"POLYGON_GROUPS filter active: {len(TICKERS)} tickers from {_group_names}")

# Optional watchlist override for all polygon jobs (essential for the free tier)
_poly_watchlist = os.getenv("POLYGON_TICKERS", "")
if _poly_watchlist:
    _want = {s.strip().upper() for s in _poly_watchlist.split(",")}
    POLYGON_TICKERS = [t for t in TICKERS if t.get("symbol", "").upper() in _want]
    logger.info(f"POLYGON_TICKERS override active: {len(POLYGON_TICKERS)} tickers")
else:
    POLYGON_TICKERS = TICKERS

# Separate watchlist for tick data — high volume, keep small
# If POLYGON_TICK_TICKERS is not set but POLYGON_GROUPS is, use same tickers for ticks
_tick_watchlist = os.getenv("POLYGON_TICK_TICKERS", "")
if _tick_watchlist:
    _want_tick = {s.strip().upper() for s in _tick_watchlist.split(",")}
    POLYGON_TICK_TICKERS = [t for t in TICKERS if t.get("symbol", "").upper() in _want_tick]
elif _polygon_groups:
    # When filtering by groups, use same tickers for tick data — ensure POLYGON_TICK_TICKERS
    # is set explicitly via env var if tick volume is a concern
    logger.warning(f"POLYGON_TICK_TICKERS not set — tick data will run for all {len(TICKERS)} group tickers. Set POLYGON_TICK_TICKERS to limit scope.")
    POLYGON_TICK_TICKERS = TICKERS
else:
    POLYGON_TICK_TICKERS = POLYGON_TICKERS

# ── Logging ───────────────────────────────────────────────────────────────────
logger.remove()
logger.add(sys.stderr, level=LOG_LEVEL,
           format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
logger.add("logs/etl_{time:YYYY-MM-DD}.log", rotation="1 day",
           retention="14 days", level="DEBUG")


# ── Lazy import after env is loaded ──────────────────────────────────────────
from db.database import init_db, get_connection
from etl.ibkr_client import IBKRClient
from etl.extract_stocks import run_stock_etl
from etl.extract_options import refresh_option_chains, run_option_etl
from etl.polygon_client import get_polygon_client
from etl.extract_polygon import (
    run_polygon_bars_etl,
    run_polygon_snapshots_etl,
    run_polygon_options_etl,
    run_polygon_option_bars_etl,
    run_polygon_reference_etl,
)
from etl.extract_polygon_ticks import run_polygon_ticks_etl
from etl.embed_tickers import run_embed_tickers_etl
from etl.extract_edgar import run_edgar_filings_etl, run_edgar_facts_etl
from etl.embed_edgar import run_embed_edgar_etl
from etl.extract_cot import run_cot_etl

from etl.utils import utcnow as _utcnow

# ── ETL helpers ───────────────────────────────────────────────────────────────

def _log_run(conn, run_type: str, status: str,
             message: str, rows: int, started: str):
    conn.execute("""
        INSERT INTO etl_runs (run_type, status, message, rows_written, started_at, finished_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (run_type, status, message, rows, started, _utcnow()))
    conn.commit()


def etl_job(run_type: str):
    """Decorator to log ETL run status and timing to the DB."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            started = _utcnow()
            try:
                rows = func(*args, **kwargs)
                conn = get_connection()
                try:
                    _log_run(conn, run_type, "ok", f"{rows} rows", rows, started)
                finally:
                    conn.close()
                return rows
            except Exception as e:
                logger.error(f"{run_type.capitalize()} ETL failed: {e}")
                conn = get_connection()
                try:
                    _log_run(conn, run_type, "error", str(e), 0, started)
                finally:
                    conn.close()
                raise
        return wrapper
    return decorator


@etl_job("stocks")
def job_stocks(client: IBKRClient):
    return run_stock_etl(client, TICKERS)


@etl_job("chain")
def job_chain(client: IBKRClient):
    return refresh_option_chains(client, TICKERS)


@etl_job("options")
def job_options(client: IBKRClient):
    return run_option_etl(client, TICKERS, EXPIRY_CYCLES)


def _polygon_client_or_exit():
    try:
        return get_polygon_client()
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)


@etl_job("polygon-bars")
def job_polygon_bars():
    poly = _polygon_client_or_exit()
    return run_polygon_bars_etl(
        poly, POLYGON_TICKERS, POLYGON_TIMESPAN, POLYGON_LOOKBACK,
        from_date=POLYGON_START_DATE, to_date=POLYGON_END_DATE,
    )


@etl_job("polygon-quotes")
def job_polygon_snapshots():
    poly = _polygon_client_or_exit()
    return run_polygon_snapshots_etl(poly, POLYGON_TICKERS)


@etl_job("polygon-options")
def job_polygon_options():
    poly = _polygon_client_or_exit()
    return run_polygon_options_etl(poly, POLYGON_TICKERS,
                                   max_per_ticker=POLYGON_OPTIONS_MAX_CONTRACTS)


@etl_job("polygon-option-bars")
def job_polygon_option_bars():
    poly = _polygon_client_or_exit()
    return run_polygon_option_bars_etl(
        poly, POLYGON_TICKERS,
        timespan=POLYGON_TIMESPAN,
        lookback_days=POLYGON_OPT_LOOKBACK,
        max_contracts=POLYGON_OPT_MAX_CONTRACTS,
    )


@etl_job("polygon-ref")
def job_polygon_reference():
    poly = _polygon_client_or_exit()
    return run_polygon_reference_etl(poly, POLYGON_TICKERS)


@etl_job("polygon-ticks")
def job_polygon_ticks():
    if not POLYGON_START_DATE or not POLYGON_END_DATE:
        logger.error("polygon-ticks requires START_DATE and END_DATE in .env")
        return 0
    poly = _polygon_client_or_exit()
    return run_polygon_ticks_etl(
        poly, POLYGON_TICK_TICKERS,
        from_date=POLYGON_START_DATE,
        to_date=POLYGON_END_DATE,
        max_per_ticker=POLYGON_TICK_MAX,
    )


@etl_job("polygon-semis")
def job_polygon_semis():
    """Run both day bars and tick data for semiconductor tickers."""
    if not POLYGON_START_DATE or not POLYGON_END_DATE:
        logger.error("polygon-semis requires START_DATE and END_DATE in .env")
        return 0
    poly = _polygon_client_or_exit()
    bars_count = run_polygon_bars_etl(
        poly, POLYGON_TICKERS, POLYGON_TIMESPAN, POLYGON_LOOKBACK,
        from_date=POLYGON_START_DATE, to_date=POLYGON_END_DATE,
    )
    ticks_count = run_polygon_ticks_etl(
        poly, POLYGON_TICK_TICKERS,
        from_date=POLYGON_START_DATE,
        to_date=POLYGON_END_DATE,
        max_per_ticker=POLYGON_TICK_MAX,
    )
    return (bars_count or 0) + (ticks_count or 0)


def job_polygon_all():
    job_polygon_reference()
    job_polygon_bars()
    job_polygon_snapshots()
    job_polygon_options()
    
    # Run ticks if explicitly requested or when using group filtering
    data_type = os.getenv("POLYGON_DATA_TYPE", "bar").lower()
    if data_type == "tick" or _polygon_groups:
        job_polygon_ticks()


@etl_job("embed-tickers")
def job_embed_tickers():
    return run_embed_tickers_etl()


@etl_job("edgar-filings")
def job_edgar_filings():
    return run_edgar_filings_etl(TICKERS)


@etl_job("edgar-facts")
def job_edgar_facts():
    return run_edgar_facts_etl(TICKERS)


@etl_job("embed-edgar")
def job_embed_edgar():
    return run_embed_edgar_etl(TICKER_SYMBOLS)


@etl_job("cot")
def job_cot():
    return run_cot_etl()


def run_all(client: IBKRClient, refresh_chain: bool = False):
    if refresh_chain:
        logger.info("── Phase 1: Refreshing option chains ──")
        job_chain(client)

    logger.info("── Phase 2: Stock quotes ──")
    job_stocks(client)

    logger.info("── Phase 3: Option quotes ──")
    job_options(client)

    logger.info("── Phase 4: COT reports ──")
    job_cot()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="IBKR + Polygon ETL Pipeline")
    parser.add_argument("--job",
                        choices=[
                            "stocks", "options", "chain", "all",
                            "polygon", "polygon-bars", "polygon-quotes",
                            "polygon-options", "polygon-option-bars", "polygon-ref",
                            "polygon-ticks", "polygon-semis",
                            "embed-tickers", "embed-edgar",
                            "edgar-filings", "edgar-facts", "cot",
                        ],
                        default="all")
    parser.add_argument("--schedule", action="store_true",
                        help="Run continuously on POLL_INTERVAL_SECONDS")
    parser.add_argument("--refresh-chain", action="store_true",
                        help="Re-fetch option chain metadata before quoting")
    args = parser.parse_args()

    # Initialise databases
    init_db()

    # Jobs that don't need a TWS connection
    polygon_only_jobs = {
        "polygon":              job_polygon_all,
        "polygon-bars":         job_polygon_bars,
        "polygon-quotes":       job_polygon_snapshots,
        "polygon-options":      job_polygon_options,
        "polygon-option-bars":  job_polygon_option_bars,
        "polygon-ref":          job_polygon_reference,
        "polygon-ticks":        job_polygon_ticks,
        "polygon-semis":        job_polygon_semis,
        "embed-tickers":        job_embed_tickers,
        "embed-edgar":          job_embed_edgar,
        "edgar-filings":        job_edgar_filings,
        "edgar-facts":          job_edgar_facts,
        "cot":                  job_cot,
    }
    if args.job in polygon_only_jobs:
        fn = polygon_only_jobs[args.job]
        if not args.schedule:
            fn()
        else:
            logger.info(f"Scheduled mode: running every {POLL_SECS}s (Ctrl-C to stop)")
            fn()
            schedule.every(POLL_SECS).seconds.do(fn)
            try:
                while True:
                    schedule.run_pending()
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Stopped by user")
        return

    # IBKR jobs require a TWS connection
    client = IBKRClient(TWS_HOST, TWS_PORT, TWS_CLIENT)
    try:
        client.connect_and_run()
    except ConnectionError as e:
        logger.error(str(e))
        sys.exit(1)

    dispatch = {
        "stocks":  lambda: job_stocks(client),
        "options": lambda: job_options(client),
        "chain":   lambda: job_chain(client),
        "all":     lambda: run_all(client, refresh_chain=args.refresh_chain),
    }
    fn = dispatch[args.job]

    try:
        if not args.schedule:
            fn()
        else:
            logger.info(f"Scheduled mode: running every {POLL_SECS}s (Ctrl-C to stop)")
            fn()   # run immediately on start
            schedule.every(POLL_SECS).seconds.do(fn)
            # Re-fetch chain once per day
            schedule.every().day.at("09:00").do(lambda: job_chain(client))
            try:
                while True:
                    schedule.run_pending()
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Stopped by user")
    finally:
        client.disconnect_and_stop()


if __name__ == "__main__":
    main()
