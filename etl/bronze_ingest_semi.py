"""
etl/bronze_ingest_semi.py
Bronze-layer Polygon ingestion for the fixed semiconductor ticker universe.

Loads the universe from config/semi_universe.json and lands raw daily OHLCV
bars into the existing DuckDB (polygon_bars) via run_polygon_bars_etl, then
records a single row in etl_runs summarising the run.

Usage:
    # Default 5-year (1825-day) lookback for the whole universe
    python -m etl.bronze_ingest_semi

    # Explicit date window
    python -m etl.bronze_ingest_semi --from 2020-01-01 --to 2025-01-01

    # Custom lookback window
    python -m etl.bronze_ingest_semi --lookback-days 365

    # Only ingest tickers that currently have zero rows in polygon_bars
    python -m etl.bronze_ingest_semi --only-missing

    # Override the universe config path
    python -m etl.bronze_ingest_semi --universe config/semi_universe.json
"""
import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

from db.database import get_connection, init_db
from etl.extract_polygon import run_polygon_bars_etl
from etl.polygon_client import get_polygon_client

logger.add("logs/etl_{time:YYYY-MM-DD}.log", rotation="1 day", retention="14 days", level="DEBUG")

# Repo root = parent of the etl/ package directory
_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_UNIVERSE = "config/semi_universe.json"

try:
    from etl.utils import utcnow as _utcnow
except ImportError:  # pragma: no cover - utils always present, defensive only
    def _utcnow() -> str:
        return datetime.now(timezone.utc).isoformat()


def load_universe(universe_path: str):
    """Load (tickers, sec_type) from a universe JSON config file."""
    path = Path(universe_path)
    if not path.is_absolute():
        path = _REPO_ROOT / path
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    sec_type = cfg.get("sec_type", "STK")
    symbols = cfg.get("tickers", [])
    tickers = [{"symbol": t, "secType": sec_type} for t in symbols]
    return tickers, sec_type


def _existing_tickers() -> set:
    """Return the set of DISTINCT tickers already present in polygon_bars."""
    with get_connection() as conn:
        rows = conn.execute("SELECT DISTINCT ticker FROM polygon_bars").fetchall()
    return {r[0] for r in rows}


def _log_etl_run(run_type: str, status: str, message: str,
                 rows_written: int, started_at: str, finished_at: str):
    """Write a single row into etl_runs."""
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO etl_runs
                (run_type, status, message, rows_written, started_at, finished_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (run_type, status, message, rows_written, started_at, finished_at))
        conn.commit()


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Bronze-layer Polygon daily-bar ingestion for the semiconductor universe."
    )
    parser.add_argument("--timespan", default="day",
                        help="Bar timespan (default: day)")
    parser.add_argument("--from", dest="from_date", default=None,
                        help="Start date (ISO, e.g. 2020-01-01)")
    parser.add_argument("--to", dest="to_date", default=None,
                        help="End date (ISO, e.g. 2025-01-01)")
    parser.add_argument("--lookback-days", type=int,
                        default=int(os.getenv("POLYGON_BARS_LOOKBACK", "1825")),
                        help="Lookback window in days when --from/--to not given "
                             "(default: env POLYGON_BARS_LOOKBACK or 1825)")
    parser.add_argument("--only-missing", action="store_true",
                        help="Ingest only tickers with zero rows in polygon_bars")
    parser.add_argument("--universe", default=_DEFAULT_UNIVERSE,
                        help=f"Universe config path (default: {_DEFAULT_UNIVERSE})")
    args = parser.parse_args()

    init_db()

    tickers, sec_type = load_universe(args.universe)
    logger.info(f"Loaded {len(tickers)} {sec_type} tickers from {args.universe}")

    if args.only_missing:
        existing = _existing_tickers()
        tickers = [t for t in tickers if t["symbol"] not in existing]
        logger.info(f"--only-missing: {len(tickers)} tickers with zero rows to ingest")

    # Describe the ingestion window for the etl_runs message, resolving the
    # same defaults run_polygon_bars_etl applies so the audit trail is accurate.
    if args.from_date or args.to_date:
        resolved_from = args.from_date or (
            date.today() - timedelta(days=args.lookback_days)
        ).isoformat()
        resolved_to = args.to_date or date.today().isoformat()
        window = f"from={resolved_from} to={resolved_to}"
    else:
        window = f"lookback_days={args.lookback_days} (to=today)"

    symbols = ", ".join(t["symbol"] for t in tickers)
    started_at = _utcnow()

    if not tickers:
        finished_at = _utcnow()
        message = f"No tickers to ingest ({window}); universe={args.universe}"
        logger.info(message)
        _log_etl_run("polygon_bars_bronze", "ok", message, 0, started_at, finished_at)
        print(f"[bronze_ingest_semi] {message}")
        return

    try:
        rows = run_polygon_bars_etl(
            get_polygon_client(),
            tickers,
            timespan=args.timespan,
            lookback_days=args.lookback_days,
            from_date=args.from_date,
            to_date=args.to_date,
        )
        finished_at = _utcnow()
        message = (
            f"timespan={args.timespan}; {window}; "
            f"{len(tickers)} tickers [{symbols}]"
        )
        _log_etl_run("polygon_bars_bronze", "ok", message, rows, started_at, finished_at)
        summary = (
            f"[bronze_ingest_semi] OK — {rows:,} rows across {len(tickers)} tickers "
            f"({args.timespan}, {window})"
        )
        logger.info(summary)
        print(summary)
    except Exception as e:
        finished_at = _utcnow()
        message = (
            f"FAILED: {e} | timespan={args.timespan}; {window}; "
            f"{len(tickers)} tickers [{symbols}]"
        )
        _log_etl_run("polygon_bars_bronze", "error", message, 0, started_at, finished_at)
        logger.error(message)
        print(f"[bronze_ingest_semi] ERROR — {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
