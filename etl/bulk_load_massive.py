"""
etl/bulk_load_massive.py
Download minute-bar flat files from Massive S3 and load selected tickers into polygon_bars.

Usage:
    python -m etl.bulk_load_massive --start 2021 --end 2026
    python -m etl.bulk_load_massive --start 2021 --end 2026 --skip-download
"""
import argparse
import gzip
import csv
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from db.database import get_connection

logger.add("logs/etl_{time:YYYY-MM-DD}.log", rotation="1 day", retention="14 days", level="DEBUG")

S3_ENDPOINT  = "https://files.massive.com"
S3_BUCKET    = "s3://flatfiles/us_stocks_sip/minute_aggs_v1"
AWS_PROFILE  = "massive"
AWS_CLI      = r"C:\Program Files\Amazon\AWSCLIV2\aws.exe"
DOWNLOAD_DIR = Path("data/minute_aggs")

TICKERS = {
    # Mag 7
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA",
    # Semiconductors
    "AMD", "INTC", "QCOM", "AVGO", "TXN", "MRVL", "MU", "SNDK",
    "AMAT", "LRCX", "KLAC", "ASML", "TSM", "ON", "MPWR",
    "NXPI", "ADI", "MCHP", "SWKS", "QRVO", "ENTG", "CRUS",
    "WOLF", "ONTO", "ACLS", "SLAB", "STM",
}


def _ns_to_iso(ns: int) -> str:
    """Convert nanosecond timestamp to ISO-8601 UTC string."""
    return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc).isoformat(timespec="seconds")


def download_year(year: int):
    """Sync all daily files for a given year from S3."""
    dest = DOWNLOAD_DIR / str(year)
    dest.mkdir(parents=True, exist_ok=True)
    logger.info(f"Syncing {S3_BUCKET}/{year}/ → {dest}")
    subprocess.run([
        AWS_CLI, "s3", "sync", f"{S3_BUCKET}/{year}/", str(dest),
        "--endpoint-url", S3_ENDPOINT,
        "--profile", AWS_PROFILE,
        "--no-progress",
    ])


def load_file(path: Path, conn) -> int:
    """Load rows for selected tickers from a .csv.gz file. Returns rows inserted."""
    rows = 0
    with gzip.open(path, "rt") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["ticker"] not in TICKERS:
                continue
            try:
                ts = _ns_to_iso(int(row["window_start"]))
                conn.execute("""
                    INSERT OR IGNORE INTO polygon_bars
                        (ticker, ts, timespan, open, high, low, close, volume, transactions)
                    VALUES (?, ?, 'minute', ?, ?, ?, ?, ?, ?)
                """, (
                    row["ticker"], ts,
                    float(row["open"]),
                    float(row["high"]),
                    float(row["low"]),
                    float(row["close"]),
                    float(row["volume"]),
                    int(row["transactions"]) if row["transactions"] else None,
                ))
                rows += 1
            except Exception as e:
                logger.warning(f"Skipping row in {path.name}: {e}")
    conn.commit()
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=2021)
    parser.add_argument("--end",   type=int, default=2026)
    parser.add_argument("--skip-download", action="store_true",
                        help="Skip S3 sync, only load already-downloaded files")
    args = parser.parse_args()

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # ── Download ──────────────────────────────────────────────────
    if not args.skip_download:
        for year in range(args.start, args.end + 1):
            download_year(year)

    # ── Load ──────────────────────────────────────────────────────
    files = sorted(f for f in DOWNLOAD_DIR.rglob("*.csv.gz")
                   if args.start <= int(f.parent.parent.name) <= args.end)

    logger.info(f"Loading {len(files)} files — filtering to {len(TICKERS)} tickers")
    total = 0
    with get_connection() as conn:
        for i, path in enumerate(files, 1):
            rows = load_file(path, conn)
            total += rows
            if i % 50 == 0 or i == len(files):
                logger.info(f"Progress: {i}/{len(files)} files — {total:,} rows inserted")

    logger.info(f"Done. {total:,} rows inserted across {len(TICKERS)} tickers")


if __name__ == "__main__":
    main()
