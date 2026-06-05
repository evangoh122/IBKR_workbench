"""
etl/bulk_load_massive.py
Download minute-bar flat files from Massive S3 and load selected tickers into polygon_bars.

Usage:
    python -m etl.bulk_load_massive --start 2021 --end 2026
    python -m etl.bulk_load_massive --start 2021 --end 2026 --skip-download
"""
import argparse
import subprocess
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


def load_all(start: int, end: int):
    """
    Load all .csv.gz files using DuckDB's native reader.
    Filters to TICKERS in-database — much faster than row-by-row Python.
    window_start is nanoseconds; convert to ISO-8601 via epoch_ms(ns/1e6).
    """
    # Build glob pattern covering all years in range
    year_dirs = [str(DOWNLOAD_DIR / str(y)) for y in range(start, end + 1)]
    # DuckDB glob needs forward slashes
    globs = [d.replace("\\", "/") + "/**/*.csv.gz" for d in year_dirs]
    glob_expr = "', '".join(globs)

    ticker_list = ", ".join(f"'{t}'" for t in sorted(TICKERS))

    logger.info(f"Loading {start}–{end} via DuckDB native reader, filtering to {len(TICKERS)} tickers")

    with get_connection() as conn:
        conn.execute(f"""
            INSERT OR IGNORE INTO polygon_bars
                (ticker, ts, timespan, open, high, low, close, volume, transactions)
            SELECT
                ticker,
                strftime(
                    epoch_ms(CAST(window_start AS BIGINT) / 1000000),
                    '%Y-%m-%dT%H:%M:%S+00:00'
                ) AS ts,
                'minute'        AS timespan,
                CAST(open         AS DOUBLE),
                CAST(high         AS DOUBLE),
                CAST(low          AS DOUBLE),
                CAST(close        AS DOUBLE),
                CAST(volume       AS DOUBLE),
                TRY_CAST(transactions AS INTEGER)
            FROM read_csv(
                ['{glob_expr}'],
                compression = 'gzip',
                header      = true,
                columns     = {{
                    'ticker':       'VARCHAR',
                    'volume':       'VARCHAR',
                    'open':         'VARCHAR',
                    'close':        'VARCHAR',
                    'high':         'VARCHAR',
                    'low':          'VARCHAR',
                    'window_start': 'VARCHAR',
                    'transactions': 'VARCHAR'
                }}
            )
            WHERE ticker IN ({ticker_list})
        """)
        conn.commit()

        total = conn.execute(
            f"SELECT COUNT(*) FROM polygon_bars WHERE ticker IN ({ticker_list})"
        ).fetchone()[0]

    logger.info(f"Done. {total:,} rows in polygon_bars for {len(TICKERS)} tickers")
    return total


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=2022)
    parser.add_argument("--end",   type=int, default=2026)
    parser.add_argument("--skip-download", action="store_true",
                        help="Skip S3 sync, only load already-downloaded files")
    args = parser.parse_args()

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    if not args.skip_download:
        for year in range(args.start, args.end + 1):
            download_year(year)

    load_all(args.start, args.end)


if __name__ == "__main__":
    main()
