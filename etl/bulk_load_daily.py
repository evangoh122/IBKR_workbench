"""
etl/bulk_load_daily.py
Download daily-bar flat files from Polygon S3 and load the configured tickers
into polygon_bars with timespan='day'.

Sibling of etl/bulk_load_massive.py (which loads MINUTE bars). Kept separate
on purpose — do not refactor the two together.

Usage:
    python -m etl.bulk_load_daily --start 2021 --end 2026
    python -m etl.bulk_load_daily --start 2021 --end 2026 --skip-download
"""
import argparse
import csv
import gzip
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

from db.database import get_connection
from etl.bulk_load_massive import TICKERS

S3_ENDPOINT  = "https://files.massive.com"
S3_BUCKET    = "s3://flatfiles/us_stocks_sip/day_aggs_v1"
AWS_PROFILE  = "massive"
AWS_CLI      = r"C:\Program Files\Amazon\AWSCLIV2\aws.exe"
DOWNLOAD_DIR = Path("data/day_aggs")


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
    """Load rows for tickers in TICKERS from a day_aggs csv.gz. Returns rows inserted."""
    rows = 0
    with gzip.open(path, "rt") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["ticker"] not in TICKERS:
                continue
            try:
                ts = _ns_to_iso(int(row["window_start"]))
                conn.execute(
                    """
                    INSERT OR IGNORE INTO polygon_bars
                        (ticker, ts, timespan, open, high, low, close, volume, transactions)
                    VALUES (?, ?, 'day', ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["ticker"], ts,
                        float(row["open"]),
                        float(row["high"]),
                        float(row["low"]),
                        float(row["close"]),
                        float(row["volume"]),
                        int(row["transactions"]) if row["transactions"] else None,
                    ),
                )
                rows += 1
            except Exception as e:
                logger.warning(f"Skipping row in {path.name}: {e}")
    conn.commit()
    return rows


def run(start_year: int = 2021, end_year: int = 2026, skip_download: bool = False) -> int:
    """Programmatic entry point. Returns total rows inserted."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    if not skip_download:
        for year in range(start_year, end_year + 1):
            download_year(year)

    files = sorted(
        f for f in DOWNLOAD_DIR.rglob("*.csv.gz")
        if start_year <= int(f.parent.name) <= end_year
    )

    logger.info(f"bulk-load-daily: {len(files)} files, filtering to {len(TICKERS)} tickers")
    total = 0
    with get_connection() as conn:
        for i, path in enumerate(files, 1):
            total += load_file(path, conn)
            if i % 50 == 0 or i == len(files):
                logger.info(f"bulk-load-daily: {i}/{len(files)} files — {total:,} rows")
    logger.info(f"bulk-load-daily done. {total:,} rows across {len(TICKERS)} tickers")
    return total


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=2021)
    parser.add_argument("--end",   type=int, default=2026)
    parser.add_argument("--skip-download", action="store_true")
    args = parser.parse_args()
    run(start_year=args.start, end_year=args.end, skip_download=args.skip_download)


if __name__ == "__main__":
    main()
