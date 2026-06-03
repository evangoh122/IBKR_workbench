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
    python main.py --job embed-tickers   # embed polygon descriptions → DuckDB vector store
    python main.py --job edgar-filings   # EDGAR filing history (10-K, 10-Q, 8-K)
    python main.py --job edgar-facts     # EDGAR XBRL financial facts
    python main.py --schedule            # continuous mode, respects POLL_INTERVAL_SECONDS
"""
import argparse
import os
import sys
import time
from datetime import datetime, timezone

import schedule
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
TWS_HOST      = os.getenv("TWS_HOST",     "127.0.0.1")
TWS_PORT      = int(os.getenv("TWS_PORT", "7497"))
TWS_CLIENT    = int(os.getenv("TWS_CLIENT_ID", "1"))
from config.tickers import get_all_tickers, get_all_ticker_symbols
TICKERS       = get_all_tickers()          # List of dicts for IBKR jobs
TICKER_SYMBOLS = get_all_ticker_symbols()  # List of strings for REST APIs
POLL_SECS     = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
LOG_LEVEL     = os.getenv("LOG_LEVEL", "INFO")
EXPIRY_CYCLES = int(os.getenv("OPTIONS_EXPIRY_CYCLES", "2"))

POLYGON_TIMESPAN         = os.getenv("POLYGON_BARS_TIMESPAN", "day")
POLYGON_LOOKBACK         = int(os.getenv("POLYGON_BARS_LOOKBACK", "9500"))
POLYGON_OPT_MAX_CONTRACTS = int(os.getenv("POLYGON_OPTION_BARS_MAX_CONTRACTS", "250"))

# ── Logging ───────────────────────────────────────────────────────────────────
logger.remove()
logger.add(sys.stderr, level=LOG_LEVEL,
           format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
logger.add("logs/etl_{time:YYYY-MM-DD}.log", rotation="1 day",
           retention="14 days", level="DEBUG")


# ── Lazy import after env is loaded ──────────────────────────────────────────
from db.database import init_db
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
from etl.embed_tickers import run_embed_tickers_etl
from etl.extract_edgar import run_edgar_filings_etl, run_edgar_facts_etl
from etl.embed_edgar import run_embed_edgar_etl


from functools import wraps
from db.database import get_connection

# ── ETL helpers ───────────────────────────────────────────────────────────────

def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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
                with get_connection() as conn:
                    _log_run(conn, run_type, "ok", f"{rows} rows", rows, started)
                return rows
            except Exception as e:
                logger.error(f"{run_type.capitalize()} ETL failed: {e}")
                with get_connection() as conn:
                    _log_run(conn, run_type, "error", str(e), 0, started)
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
    return run_polygon_bars_etl(poly, TICKERS, POLYGON_TIMESPAN, POLYGON_LOOKBACK)


@etl_job("polygon-quotes")
def job_polygon_snapshots():
    poly = _polygon_client_or_exit()
    return run_polygon_snapshots_etl(poly, TICKERS)


@etl_job("polygon-options")
def job_polygon_options():
    poly = _polygon_client_or_exit()
    return run_polygon_options_etl(poly, TICKERS)


@etl_job("polygon-option-bars")
def job_polygon_option_bars():
    poly = _polygon_client_or_exit()
    return run_polygon_option_bars_etl(
        poly, TICKERS,
        timespan=POLYGON_TIMESPAN,
        lookback_days=POLYGON_LOOKBACK,
        max_contracts=POLYGON_OPT_MAX_CONTRACTS,
    )


@etl_job("polygon-ref")
def job_polygon_reference():
    poly = _polygon_client_or_exit()
    return run_polygon_reference_etl(poly, TICKERS)


def job_polygon_all():
    job_polygon_reference()
    job_polygon_bars()
    job_polygon_snapshots()
    job_polygon_options()


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


def run_all(client: IBKRClient, refresh_chain: bool = False):
    if refresh_chain:
        logger.info("── Phase 1: Refreshing option chains ──")
        job_chain(client)

    logger.info("── Phase 2: Stock quotes ──")
    job_stocks(client)

    logger.info("── Phase 3: Option quotes ──")
    job_options(client)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="IBKR + Polygon ETL Pipeline")
    parser.add_argument("--job",
                        choices=[
                            "stocks", "options", "chain", "all",
                            "polygon", "polygon-bars", "polygon-quotes",
                            "polygon-options", "polygon-option-bars", "polygon-ref",
                            "embed-tickers", "embed-edgar",
                            "edgar-filings", "edgar-facts",
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
        "polygon":         job_polygon_all,
        "polygon-bars":    job_polygon_bars,
        "polygon-quotes":  job_polygon_snapshots,
        "polygon-options":      job_polygon_options,
        "polygon-option-bars":  job_polygon_option_bars,
        "polygon-ref":     job_polygon_reference,
        "embed-tickers":   job_embed_tickers,
        "embed-edgar":     job_embed_edgar,
        "edgar-filings":   job_edgar_filings,
        "edgar-facts":     job_edgar_facts,
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

    client.disconnect_and_stop()


if __name__ == "__main__":
    main()
