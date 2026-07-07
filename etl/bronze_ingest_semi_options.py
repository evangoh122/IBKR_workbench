"""
etl/bronze_ingest_semi_options.py
Bronze-layer Polygon ingestion for HISTORICAL OPTIONS OHLCV bars over the fixed
semiconductor ticker universe.

Loads the universe from config/semi_universe.json and, for each underlying, lands
raw per-contract daily OHLCV + VWAP option bars into the existing DuckDB
(polygon_option_bars) via run_polygon_option_bars_etl, then records a single row
in etl_runs summarising the run.

Usage:
    # Default 730-day lookback, up to 1000 contracts per underlying
    python -m etl.bronze_ingest_semi_options

    # Custom lookback window
    python -m etl.bronze_ingest_semi_options --lookback-days 365

    # Cap contracts per underlying, or fetch as many as possible
    python -m etl.bronze_ingest_semi_options --max-contracts 250
    python -m etl.bronze_ingest_semi_options --max-contracts all

    # Only ingest underlyings that currently have zero rows in polygon_option_bars
    python -m etl.bronze_ingest_semi_options --only-missing

    # Override the universe config path
    python -m etl.bronze_ingest_semi_options --universe config/semi_universe.json
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
from etl.extract_polygon import run_polygon_option_bars_etl
from etl.polygon_client import get_polygon_client

logger.add("logs/etl_{time:YYYY-MM-DD}.log", rotation="1 day", retention="14 days", level="DEBUG")

# Sentinel int used when --max-contracts is given as the literal string "all".
_ALL_CONTRACTS = 100000

# Repo root = parent of the etl/ package directory
_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_UNIVERSE = "config/semi_universe.json"

try:
    from etl.utils import utcnow as _utcnow
except ImportError:  # pragma: no cover - utils always present, defensive only
    def _utcnow() -> str:
        return datetime.now(timezone.utc).isoformat()


def _parse_max_contracts(value: str) -> int:
    """Parse --max-contracts: an int, or the literal 'all' → a large int."""
    if isinstance(value, str) and value.strip().lower() == "all":
        return _ALL_CONTRACTS
    try:
        return int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(
            f"--max-contracts must be an integer or 'all' (got {value!r})"
        )


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


def _existing_underlyings() -> set:
    """Return the set of DISTINCT underlyings already present in polygon_option_bars."""
    with get_connection() as conn:
        rows = conn.execute("SELECT DISTINCT underlying FROM polygon_option_bars").fetchall()
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
    # WARNING: Neutralize the extractor's per-ticker override filter.
    # run_polygon_option_bars_etl reads POLYGON_OPTION_BARS_TICKERS and, if it is
    # set AND non-empty, filters the tickers we pass down to only that list. The
    # committed .env sets it to a subset that EXCLUDES most of our 34 semis. By
    # forcing it to the empty string here (after load_dotenv, before the ETL),
    # the extractor's `if override:` branch is falsy, so it processes exactly the
    # universe tickers we hand it — the whole semiconductor universe.
    os.environ["POLYGON_OPTION_BARS_TICKERS"] = ""

    parser = argparse.ArgumentParser(
        description="Bronze-layer Polygon option-bar ingestion for the semiconductor universe."
    )
    parser.add_argument("--timespan", default="day",
                        help="Bar timespan (default: day)")
    parser.add_argument("--lookback-days", type=int,
                        default=int(os.getenv("POLYGON_OPTION_BARS_LOOKBACK", "730")),
                        help="Lookback window in days "
                             "(default: env POLYGON_OPTION_BARS_LOOKBACK or 730)")
    parser.add_argument("--max-contracts", type=_parse_max_contracts,
                        default=int(os.getenv("POLYGON_OPTION_BARS_MAX_CONTRACTS", "1000")),
                        help="Max option contracts per underlying; int or 'all' "
                             "(default: env POLYGON_OPTION_BARS_MAX_CONTRACTS or 1000)")
    parser.add_argument("--only-missing", action="store_true",
                        help="Ingest only underlyings with zero rows in polygon_option_bars")
    parser.add_argument("--universe", default=_DEFAULT_UNIVERSE,
                        help=f"Universe config path (default: {_DEFAULT_UNIVERSE})")
    args = parser.parse_args()

    init_db()

    tickers, sec_type = load_universe(args.universe)
    logger.info(f"Loaded {len(tickers)} {sec_type} tickers from {args.universe}")

    if args.only_missing:
        existing = _existing_underlyings()
        tickers = [t for t in tickers if t["symbol"] not in existing]
        logger.info(f"--only-missing: {len(tickers)} underlyings with zero rows to ingest")

    window = f"lookback_days={args.lookback_days} (to=today)"
    symbols = ", ".join(t["symbol"] for t in tickers)
    started_at = _utcnow()

    if not tickers:
        finished_at = _utcnow()
        message = f"No underlyings to ingest ({window}); universe={args.universe}"
        logger.info(message)
        _log_etl_run("polygon_option_bars_bronze", "ok", message, 0, started_at, finished_at)
        print(f"[bronze_ingest_semi_options] {message}")
        return

    try:
        rows = run_polygon_option_bars_etl(
            get_polygon_client(),
            tickers,
            timespan=args.timespan,
            lookback_days=args.lookback_days,
            max_contracts=args.max_contracts,
        )
        finished_at = _utcnow()
        message = (
            f"timespan={args.timespan}; {window}; max_contracts={args.max_contracts}; "
            f"{len(tickers)} underlyings [{symbols}]"
        )
        _log_etl_run("polygon_option_bars_bronze", "ok", message, rows, started_at, finished_at)
        summary = (
            f"[bronze_ingest_semi_options] OK — {rows:,} option bar rows across "
            f"{len(tickers)} underlyings ({args.timespan}, {window}, "
            f"max_contracts={args.max_contracts})"
        )
        logger.info(summary)
        print(summary)
    except Exception as e:
        finished_at = _utcnow()
        message = (
            f"FAILED: {e} | timespan={args.timespan}; {window}; "
            f"max_contracts={args.max_contracts}; {len(tickers)} underlyings [{symbols}]"
        )
        _log_etl_run("polygon_option_bars_bronze", "error", message, 0, started_at, finished_at)
        logger.error(message)
        print(f"[bronze_ingest_semi_options] ERROR — {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
