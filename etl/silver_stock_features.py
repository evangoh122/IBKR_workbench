"""
etl/silver_stock_features.py
Silver-layer per-stock daily technical features for the semiconductor universe.

Source : polygon_bars WHERE timespan='day' AND ticker IN (universe)   (Bronze)
Target : silver_stock_features                                        (Silver)
Grain  : one row per (ticker, trade_date), rebuilt with INSERT OR REPLACE.

All features are computed in a single DuckDB pass with window functions
partitioned by ticker and ordered by trade_date. Trailing N-day windows use
min-periods = full window: a feature is emitted only once its full N-row window
is available, otherwise NULL (plan decision D3 — keep the row, NULL the feature).

Usage:
    # Rebuild features for the whole universe
    python -m etl.silver_stock_features

    # Override the universe config path
    python -m etl.silver_stock_features --universe config/semi_universe.json
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

from db.database import get_connection, init_db

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
    """Load the list of ticker symbols from a universe JSON config file."""
    path = Path(universe_path)
    if not path.is_absolute():
        path = _REPO_ROOT / path
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return list(cfg.get("tickers", []))


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


def _build_sql(placeholders: str) -> str:
    """
    Build the single INSERT ... SELECT that computes every feature.

    `placeholders` is a comma-separated string of '?' for the ticker IN-list.
    Windows (w20/w50/w100) are trailing N-row windows. Each feature is guarded
    with `CASE WHEN count(close) OVER wN = N THEN <expr> END` so partial windows
    (near the start of a ticker's history) emit NULL instead of a biased value.
    z-score and VWAP are derived from the same windowed aggregates.
    """
    return f"""
        INSERT OR REPLACE INTO silver_stock_features
            (ticker, ts, trade_date, close, volume, daily_return,
             ma_20, ma_50, ma_100,
             std_20, std_50, std_100,
             zscore_20, zscore_50, zscore_100,
             sigma_flag_20, sigma_flag_50, sigma_flag_100,
             pct_change,
             zscore_ret_20, zscore_ret_50, zscore_ret_100,
             sigma_flag_ret_20, sigma_flag_ret_50, sigma_flag_ret_100,
             vwap_20, vwap_50, vwap_100)
        WITH base AS (
            SELECT
                ticker,
                ts,
                CAST(ts AS DATE) AS trade_date,
                CAST(close  AS DOUBLE) AS close,
                CAST(volume AS DOUBLE) AS volume,
                CAST(high   AS DOUBLE) AS high,
                CAST(low    AS DOUBLE) AS low
            FROM polygon_bars
            WHERE timespan = 'day'
              AND ticker IN ({placeholders})
        ),
        rets AS (
            SELECT
                *,
                close / NULLIF(lag(close) OVER (PARTITION BY ticker ORDER BY trade_date), 0) - 1
                    AS ret
            FROM base
        ),
        feat AS (
            SELECT
                ticker,
                ts,
                trade_date,
                close,
                volume,
                ret,
                -- window row counts (drive the min-periods guards)
                count(close) OVER w20  AS n20,
                count(close) OVER w50  AS n50,
                count(close) OVER w100 AS n100,
                -- return window counts for warm-up guards on return features
                count(ret) OVER w20  AS n_ret20,
                count(ret) OVER w50  AS n_ret50,
                count(ret) OVER w100 AS n_ret100,
                -- moving averages
                avg(close) OVER w20    AS ma20_raw,
                avg(close) OVER w50    AS ma50_raw,
                avg(close) OVER w100   AS ma100_raw,
                -- rolling sample std of close
                stddev_samp(close) OVER w20   AS std20_raw,
                stddev_samp(close) OVER w50   AS std50_raw,
                stddev_samp(close) OVER w100  AS std100_raw,
                -- rolling return stats for return z-scores
                avg(ret) OVER w20   AS mean_ret20,
                avg(ret) OVER w50   AS mean_ret50,
                avg(ret) OVER w100  AS mean_ret100,
                stddev_samp(ret) OVER w20   AS std_ret20,
                stddev_samp(ret) OVER w50   AS std_ret50,
                stddev_samp(ret) OVER w100  AS std_ret100,
                -- rolling VWAP numerator/denominator, typical = (h+l+c)/3
                sum(((high + low + close) / 3.0) * volume) OVER w20   AS tpv20,
                sum(((high + low + close) / 3.0) * volume) OVER w50   AS tpv50,
                sum(((high + low + close) / 3.0) * volume) OVER w100  AS tpv100,
                sum(volume) OVER w20   AS vol20,
                sum(volume) OVER w50   AS vol50,
                sum(volume) OVER w100  AS vol100
            FROM rets
            WINDOW
                w20  AS (PARTITION BY ticker ORDER BY trade_date
                         ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                w50  AS (PARTITION BY ticker ORDER BY trade_date
                         ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),
                w100 AS (PARTITION BY ticker ORDER BY trade_date
                         ROWS BETWEEN 99 PRECEDING AND CURRENT ROW)
        )
        SELECT
            ticker,
            ts,
            trade_date,
            close,
            volume,
            ret AS daily_return,
            CASE WHEN n20  = 20  THEN ma20_raw  END AS ma_20,
            CASE WHEN n50  = 50  THEN ma50_raw  END AS ma_50,
            CASE WHEN n100 = 100 THEN ma100_raw END AS ma_100,
            CASE WHEN n20  = 20  THEN std20_raw  END AS std_20,
            CASE WHEN n50  = 50  THEN std50_raw  END AS std_50,
            CASE WHEN n100 = 100 THEN std100_raw END AS std_100,
            CASE WHEN n20  = 20  THEN (close - ma20_raw)  / NULLIF(std20_raw,  0) END AS zscore_20,
            CASE WHEN n50  = 50  THEN (close - ma50_raw)  / NULLIF(std50_raw,  0) END AS zscore_50,
            CASE WHEN n100 = 100 THEN (close - ma100_raw) / NULLIF(std100_raw, 0) END AS zscore_100,
            -- +-3s sigma band flags (close price)
            CASE
                WHEN n20 < 20 THEN NULL
                WHEN (close - ma20_raw) / NULLIF(std20_raw, 0) >  3 THEN '+3s'
                WHEN (close - ma20_raw) / NULLIF(std20_raw, 0) < -3 THEN '-3s'
                ELSE 'normal'
            END AS sigma_flag_20,
            CASE
                WHEN n50 < 50 THEN NULL
                WHEN (close - ma50_raw) / NULLIF(std50_raw, 0) >  3 THEN '+3s'
                WHEN (close - ma50_raw) / NULLIF(std50_raw, 0) < -3 THEN '-3s'
                ELSE 'normal'
            END AS sigma_flag_50,
            CASE
                WHEN n100 < 100 THEN NULL
                WHEN (close - ma100_raw) / NULLIF(std100_raw, 0) >  3 THEN '+3s'
                WHEN (close - ma100_raw) / NULLIF(std100_raw, 0) < -3 THEN '-3s'
                ELSE 'normal'
            END AS sigma_flag_100,
            -- pct_change: daily return expressed as percent
            ret * 100 AS pct_change,
            -- return z-scores: how unusual is today's return vs recent distribution
            CASE WHEN n_ret20  = 20  THEN (ret - mean_ret20)  / NULLIF(std_ret20,  0) END AS zscore_ret_20,
            CASE WHEN n_ret50  = 50  THEN (ret - mean_ret50)  / NULLIF(std_ret50,  0) END AS zscore_ret_50,
            CASE WHEN n_ret100 = 100 THEN (ret - mean_ret100) / NULLIF(std_ret100, 0) END AS zscore_ret_100,
            -- sigma flags on returns
            CASE
                WHEN n_ret20 < 20 THEN NULL
                WHEN (ret - mean_ret20) / NULLIF(std_ret20, 0) >  3 THEN '+3s'
                WHEN (ret - mean_ret20) / NULLIF(std_ret20, 0) < -3 THEN '-3s'
                ELSE 'normal'
            END AS sigma_flag_ret_20,
            CASE
                WHEN n_ret50 < 50 THEN NULL
                WHEN (ret - mean_ret50) / NULLIF(std_ret50, 0) >  3 THEN '+3s'
                WHEN (ret - mean_ret50) / NULLIF(std_ret50, 0) < -3 THEN '-3s'
                ELSE 'normal'
            END AS sigma_flag_ret_50,
            CASE
                WHEN n_ret100 < 100 THEN NULL
                WHEN (ret - mean_ret100) / NULLIF(std_ret100, 0) >  3 THEN '+3s'
                WHEN (ret - mean_ret100) / NULLIF(std_ret100, 0) < -3 THEN '-3s'
                ELSE 'normal'
            END AS sigma_flag_ret_100,
            CASE WHEN n20  = 20  THEN tpv20  / NULLIF(vol20,  0) END AS vwap_20,
            CASE WHEN n50  = 50  THEN tpv50  / NULLIF(vol50,  0) END AS vwap_50,
            CASE WHEN n100 = 100 THEN tpv100 / NULLIF(vol100, 0) END AS vwap_100
        FROM feat
    """


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Silver-layer per-stock daily technical features "
                    "for the semiconductor universe."
    )
    parser.add_argument("--universe", default=_DEFAULT_UNIVERSE,
                        help=f"Universe config path (default: {_DEFAULT_UNIVERSE})")
    args = parser.parse_args()

    init_db()

    tickers = load_universe(args.universe)
    logger.info(f"Loaded {len(tickers)} tickers from {args.universe}")
    started_at = _utcnow()

    if not tickers:
        finished_at = _utcnow()
        message = f"No tickers in universe={args.universe}; nothing to build"
        logger.info(message)
        _log_etl_run("silver_stock_features", "ok", message, 0, started_at, finished_at)
        print(f"[silver_stock_features] {message}")
        return

    placeholders = ", ".join(["?"] * len(tickers))
    try:
        with get_connection() as conn:
            conn.execute(
                f"DELETE FROM silver_stock_features WHERE ticker IN ({placeholders})",
                tickers,
            )
            conn.execute(_build_sql(placeholders), tickers)
            # Count rows for this universe after the rebuild.
            rows = conn.execute(
                f"SELECT COUNT(*) FROM silver_stock_features WHERE ticker IN ({placeholders})",
                tickers,
            ).fetchone()[0]
            conn.commit()
        finished_at = _utcnow()
        message = f"{rows} rows across {len(tickers)} tickers from {args.universe}"
        _log_etl_run("silver_stock_features", "ok", message, rows, started_at, finished_at)
        summary = (
            f"[silver_stock_features] OK — {rows:,} rows across "
            f"{len(tickers)} tickers"
        )
        logger.info(summary)
        print(summary)
    except Exception as e:
        finished_at = _utcnow()
        message = f"FAILED: {e} | {len(tickers)} tickers from {args.universe}"
        _log_etl_run("silver_stock_features", "error", message, 0, started_at, finished_at)
        logger.error(message)
        print(f"[silver_stock_features] ERROR — {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
