"""
etl/bulk_load_flatfiles.py
Bronze-layer loader for Polygon S3 FLAT FILES (day aggregates) for the
semiconductor universe.

Ingests, using the Massive S3 flat files (NOT the REST API):
  * STOCK   daily OHLCV bars → DuckDB polygon_bars        (timespan='day')
  * OPTIONS daily OHLCV bars → DuckDB polygon_option_bars (timespan='day')

Flat-file layout (verified via `aws s3 ls`):
  * Stocks : s3://flatfiles/us_stocks_sip/day_aggs_v1/{YYYY}/{MM}/{YYYY-MM-DD}.csv.gz
  * Options: s3://flatfiles/us_options_opra/day_aggs_v1/{YYYY}/{MM}/{YYYY-MM-DD}.csv.gz
  * CSV (gzip, header): ticker,volume,open,close,high,low,window_start,transactions
    - window_start is NANOSECONDS since epoch
    - NO vwap column in flat files (vwap left NULL)
    - Options `ticker` is an OPRA symbol, e.g. O:MU240119C00080000
      = underlying MU, expiry 2024-01-19, Call, strike 80.000

Usage:
    python -m etl.bulk_load_flatfiles --dataset both    --start-year 2021 --end-year 2026
    python -m etl.bulk_load_flatfiles --dataset stocks  --start-year 2021 --end-year 2026
    python -m etl.bulk_load_flatfiles --dataset options --start-year 2024 --end-year 2026
    python -m etl.bulk_load_flatfiles --dataset both --skip-download
"""
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

from db.database import get_connection, init_db

logger.add("logs/etl_{time:YYYY-MM-DD}.log", rotation="1 day", retention="14 days", level="DEBUG")

# ── S3 / AWS constants (env-overridable so the loader runs on Linux CI too) ───
S3_ENDPOINT  = os.getenv("S3_ENDPOINT", "https://files.massive.com")
AWS_PROFILE  = os.getenv("AWS_PROFILE", "massive")
# On Windows default to the installed exe; on Linux CI set AWS_CLI=aws (on PATH).
AWS_CLI      = os.getenv("AWS_CLI", r"C:\Program Files\Amazon\AWSCLIV2\aws.exe")

# ── Dataset bucket roots + local download dirs ───────────────────────────────
STOCKS_BUCKET   = "s3://flatfiles/us_stocks_sip/day_aggs_v1"
OPTIONS_BUCKET  = "s3://flatfiles/us_options_opra/day_aggs_v1"
STOCKS_DIR      = Path("data/day_aggs_stocks")
OPTIONS_DIR     = Path("data/day_aggs_options")

# dataset key → (bucket root, local download dir)
_DATASETS = {
    "stocks":  (STOCKS_BUCKET,  STOCKS_DIR),
    "options": (OPTIONS_BUCKET, OPTIONS_DIR),
}

# Repo root = parent of the etl/ package directory
_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_UNIVERSE = "config/semi_universe.json"

try:
    from etl.utils import utcnow as _utcnow
except ImportError:  # pragma: no cover - utils always present, defensive only
    def _utcnow() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ── Universe loading ─────────────────────────────────────────────────────────
def load_universe(universe_path: str):
    """Return the list of ticker symbols from a universe JSON config file."""
    path = Path(universe_path)
    if not path.is_absolute():
        path = _REPO_ROOT / path
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg.get("tickers", [])


def _sql_in_list(symbols) -> str:
    """
    Build a safe SQL IN-list literal from ticker symbols.
    Uppercase and keep only alphanumerics / dot to prevent injection.
    """
    safe = []
    for s in symbols:
        cleaned = "".join(ch for ch in str(s).upper() if ch.isalnum() or ch == ".")
        if cleaned:
            safe.append(cleaned)
    # de-dup, stable order
    seen = {}
    for s in safe:
        seen[s] = None
    return ", ".join(f"'{s}'" for s in seen)


def _year_globs(dest: Path, start_year: int, end_year: int) -> list:
    """
    Build DuckDB glob patterns over each year dir's **/*.csv.gz, INCLUDING ONLY
    year dirs that exist and contain at least one .csv.gz. Skipping empty/missing
    years avoids DuckDB's "No files found that match the pattern" abort (M1).
    """
    globs = []
    for y in range(start_year, end_year + 1):
        ydir = dest / str(y)
        if ydir.exists() and any(ydir.rglob("*.csv.gz")):
            # DuckDB glob needs forward slashes
            globs.append(str(ydir).replace("\\", "/") + "/**/*.csv.gz")
    return globs


# ── ETL run logging (same pattern as etl/bronze_ingest_semi.py) ───────────────
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


# ── Download ─────────────────────────────────────────────────────────────────
def download_year(dataset: str, year: int):
    """Sync all daily flat files for a dataset/year from S3 (mirrors bulk_load_massive)."""
    bucket, dest_root = _DATASETS[dataset]
    dest = dest_root / str(year)
    dest.mkdir(parents=True, exist_ok=True)
    logger.info(f"Syncing {bucket}/{year}/ → {dest}")
    result = subprocess.run([
        AWS_CLI, "s3", "sync", f"{bucket}/{year}/", str(dest),
        "--endpoint-url", S3_ENDPOINT,
        "--profile", AWS_PROFILE,
        "--no-progress",
    ])
    if result.returncode != 0:
        logger.warning(f"s3 sync for {dataset}/{year} exited {result.returncode} — "
                       f"files may be incomplete (continuing with what downloaded)")


# ── Stock loader ─────────────────────────────────────────────────────────────
def load_stocks(start_year: int, end_year: int, universe) -> int:
    """
    Load stock daily bars from the downloaded flat files into polygon_bars,
    filtered to the universe. window_start is nanoseconds → ISO-8601 UTC.
    vwap is left unset (NULL) — flat files have no vwap column.
    """
    globs = _year_globs(STOCKS_DIR, start_year, end_year)
    if not globs:
        logger.warning(f"No stock flat files found for {start_year}–{end_year} in {STOCKS_DIR}")
        return 0
    glob_expr = "', '".join(globs)
    ticker_list = _sql_in_list(universe)

    logger.info(
        f"Loading STOCK day_aggs {start_year}–{end_year} via DuckDB, "
        f"filtering to {len(universe)} universe tickers"
    )

    with get_connection() as conn:
        conn.execute(f"""
            INSERT OR IGNORE INTO polygon_bars
                (ticker, ts, timespan, open, high, low, close, volume, transactions)
            SELECT
                ticker,
                strftime(
                    make_timestamp(CAST(window_start AS BIGINT) // 1000),
                    '%Y-%m-%dT%H:%M:%S+00:00'
                ) AS ts,
                'day'                        AS timespan,
                CAST(open   AS DOUBLE),
                CAST(high   AS DOUBLE),
                CAST(low    AS DOUBLE),
                CAST(close  AS DOUBLE),
                CAST(volume AS DOUBLE),
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

        row = conn.execute(
            f"SELECT COUNT(*) FROM polygon_bars "
            f"WHERE timespan = 'day' AND ticker IN ({ticker_list})"
        ).fetchone()
        total = row[0] if row else 0

    logger.info(f"Done. {total:,} day rows in polygon_bars for {len(universe)} tickers")
    return total


# ── Options loader ───────────────────────────────────────────────────────────
def load_options(start_year: int, end_year: int, universe) -> int:
    """
    Load OPTION daily bars from the downloaded flat files into
    polygon_option_bars, parsing the OPRA ticker and filtering on the parsed
    underlying. window_start is nanoseconds → ISO-8601 UTC. vwap left NULL.

    OPRA ticker format: O:{UNDERLYING}{YYMMDD}{C|P}{STRIKE8}
      e.g. O:MU240119C00080000 → MU / 2024-01-19 / call / 80.000
    """
    globs = _year_globs(OPTIONS_DIR, start_year, end_year)
    if not globs:
        logger.warning(f"No option flat files found for {start_year}–{end_year} in {OPTIONS_DIR}")
        return 0
    glob_expr = "', '".join(globs)
    underlying_list = _sql_in_list(universe)

    logger.info(
        f"Loading OPTION day_aggs {start_year}–{end_year} via DuckDB, "
        f"filtering to {len(universe)} universe underlyings"
    )

    with get_connection() as conn:
        conn.execute(f"""
            INSERT OR IGNORE INTO polygon_option_bars
                (option_ticker, underlying, expiry, strike, "right",
                 ts, timespan, open, high, low, close, volume, transactions)
            WITH raw AS (
                SELECT
                    ticker,
                    volume, open, close, high, low, window_start, transactions,
                    regexp_extract(ticker, '^O:([A-Z.]+)([0-9]{{6}})([CP])([0-9]{{8}})$', 1) AS g_underlying,
                    regexp_extract(ticker, '^O:([A-Z.]+)([0-9]{{6}})([CP])([0-9]{{8}})$', 2) AS g_expiry,
                    regexp_extract(ticker, '^O:([A-Z.]+)([0-9]{{6}})([CP])([0-9]{{8}})$', 3) AS g_right,
                    regexp_extract(ticker, '^O:([A-Z.]+)([0-9]{{6}})([CP])([0-9]{{8}})$', 4) AS g_strike
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
            )
            SELECT
                ticker                                            AS option_ticker,
                g_underlying                                      AS underlying,
                '20' || substr(g_expiry, 1, 2) || '-'
                     || substr(g_expiry, 3, 2) || '-'
                     || substr(g_expiry, 5, 2)                    AS expiry,
                CAST(g_strike AS DOUBLE) / 1000.0                 AS strike,
                CASE WHEN g_right = 'C' THEN 'call' ELSE 'put' END AS "right",
                strftime(
                    make_timestamp(CAST(window_start AS BIGINT) // 1000),
                    '%Y-%m-%dT%H:%M:%S+00:00'
                )                                                 AS ts,
                'day'                                             AS timespan,
                CAST(open   AS DOUBLE),
                CAST(high   AS DOUBLE),
                CAST(low    AS DOUBLE),
                CAST(close  AS DOUBLE),
                CAST(volume AS DOUBLE),
                TRY_CAST(transactions AS INTEGER)
            FROM raw
            WHERE g_underlying <> ''
              AND g_underlying IN ({underlying_list})
        """)
        conn.commit()

        row = conn.execute(
            f"SELECT COUNT(*) FROM polygon_option_bars "
            f"WHERE timespan = 'day' AND underlying IN ({underlying_list})"
        ).fetchone()
        total = row[0] if row else 0

    logger.info(
        f"Done. {total:,} day rows in polygon_option_bars for {len(universe)} underlyings"
    )
    return total


# ── Orchestration ────────────────────────────────────────────────────────────
def _run_dataset(dataset: str, start_year: int, end_year: int,
                 universe, skip_download: bool):
    """Download (unless skipped) then load one dataset, logging an etl_runs row."""
    if not skip_download:
        for year in range(start_year, end_year + 1):
            download_year(dataset, year)

    if dataset == "stocks":
        run_type = "polygon_bars_bronze_flatfile"
        loader = load_stocks
    else:
        run_type = "polygon_option_bars_bronze_flatfile"
        loader = load_options

    started_at = _utcnow()
    window = f"start_year={start_year} end_year={end_year}"
    try:
        rows = loader(start_year, end_year, universe)
        finished_at = _utcnow()
        message = f"{window}; {len(universe)} universe symbols; skip_download={skip_download}"
        _log_etl_run(run_type, "ok", message, rows, started_at, finished_at)
        summary = f"[bulk_load_flatfiles] OK — {dataset}: {rows:,} rows ({window})"
        logger.info(summary)
        print(summary)
    except Exception as e:
        finished_at = _utcnow()
        message = f"FAILED: {e} | {window}; {len(universe)} universe symbols"
        _log_etl_run(run_type, "error", message, 0, started_at, finished_at)
        logger.error(message)
        print(f"[bulk_load_flatfiles] ERROR — {dataset}: {e}", file=sys.stderr)
        raise


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Bronze-layer Polygon FLAT-FILE loader (stock + option daily bars) "
                    "for the semiconductor universe."
    )
    parser.add_argument("--dataset", choices=["stocks", "options", "both"],
                        default="both", help="Which dataset(s) to load (default: both)")
    parser.add_argument("--start-year", type=int, default=2021,
                        help="First calendar year to load (default: 2021, ~5yr for stocks)")
    parser.add_argument("--end-year", type=int, default=2026,
                        help="Last calendar year to load, inclusive (default: 2026)")
    parser.add_argument("--options-start-year", type=int, default=2024,
                        help="Override first year for OPTIONS when --dataset both "
                             "(default: 2024, ~2yr). Ignored for --dataset stocks.")
    parser.add_argument("--skip-download", action="store_true",
                        help="Skip S3 sync, only load already-downloaded files")
    parser.add_argument("--universe", default=_DEFAULT_UNIVERSE,
                        help=f"Universe config path (default: {_DEFAULT_UNIVERSE})")
    args = parser.parse_args()

    init_db()

    universe = load_universe(args.universe)
    logger.info(f"Loaded {len(universe)} tickers from {args.universe}")

    STOCKS_DIR.mkdir(parents=True, exist_ok=True)
    OPTIONS_DIR.mkdir(parents=True, exist_ok=True)

    failed = False

    if args.dataset in ("stocks", "both"):
        try:
            _run_dataset("stocks", args.start_year, args.end_year, universe,
                         args.skip_download)
        except Exception:
            failed = True

    if args.dataset in ("options", "both"):
        # Options default to a shorter (2yr) window when running "both".
        opt_start = args.options_start_year if args.dataset == "both" else args.start_year
        try:
            _run_dataset("options", opt_start, args.end_year, universe,
                         args.skip_download)
        except Exception:
            failed = True

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
