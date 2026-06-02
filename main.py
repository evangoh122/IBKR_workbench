"""
main.py
Entry point for the IBKR ETL pipeline.

Usage:
    python main.py                  # run once, all jobs
    python main.py --job stocks     # only stock quotes
    python main.py --job options    # only option quotes (uses cached chain)
    python main.py --job chain      # only refresh option chain metadata
    python main.py --schedule       # continuous mode, respects POLL_INTERVAL_SECONDS
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
TWS_HOST     = os.getenv("TWS_HOST",     "127.0.0.1")
TWS_PORT     = int(os.getenv("TWS_PORT", "7497"))
TWS_CLIENT   = int(os.getenv("TWS_CLIENT_ID", "1"))
from config.tickers import get_all_tickers
TICKERS = get_all_tickers()
POLL_SECS    = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
LOG_LEVEL    = os.getenv("LOG_LEVEL", "INFO")
EXPIRY_CYCLES = int(os.getenv("OPTIONS_EXPIRY_CYCLES", "2"))

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


# ── ETL helpers ───────────────────────────────────────────────────────────────

def _log_run(conn, run_type: str, status: str,
             message: str, rows: int, started: str):
    conn.execute("""
        INSERT INTO etl_runs (run_type, status, message, rows_written, started_at, finished_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (run_type, status, message, rows, started, _utcnow()))
    conn.commit()


def job_stocks(client: IBKRClient):
    from db.database import get_connection
    started = _utcnow()
    try:
        rows = run_stock_etl(client, TICKERS)
        conn = get_connection()
        _log_run(conn, "stocks", "ok", f"{rows} rows", rows, started)
        conn.close()
    except Exception as e:
        logger.error(f"Stock ETL failed: {e}")
        conn = get_connection()
        _log_run(conn, "stocks", "error", str(e), 0, started)
        conn.close()


def job_chain(client: IBKRClient):
    from db.database import get_connection
    started = _utcnow()
    try:
        rows = refresh_option_chains(client, TICKERS)
        conn = get_connection()
        _log_run(conn, "chain", "ok", f"{rows} entries", rows, started)
        conn.close()
    except Exception as e:
        logger.error(f"Chain refresh failed: {e}")
        conn = get_connection()
        _log_run(conn, "chain", "error", str(e), 0, started)
        conn.close()


def job_options(client: IBKRClient):
    from db.database import get_connection
    started = _utcnow()
    try:
        rows = run_option_etl(client, TICKERS, EXPIRY_CYCLES)
        conn = get_connection()
        _log_run(conn, "options", "ok", f"{rows} rows", rows, started)
        conn.close()
    except Exception as e:
        logger.error(f"Options ETL failed: {e}")
        conn = get_connection()
        _log_run(conn, "options", "error", str(e), 0, started)
        conn.close()


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
    parser = argparse.ArgumentParser(description="IBKR ETL Pipeline")
    parser.add_argument("--job",
                        choices=["stocks", "options", "chain", "all"],
                        default="all")
    parser.add_argument("--schedule", action="store_true",
                        help="Run continuously on POLL_INTERVAL_SECONDS")
    parser.add_argument("--refresh-chain", action="store_true",
                        help="Re-fetch option chain metadata before quoting")
    args = parser.parse_args()

    # Initialise DB
    init_db()

    # Connect to TWS
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


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


if __name__ == "__main__":
    main()
