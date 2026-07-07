"""
etl/silver_option_positioning.py
Silver-layer per-underlying daily options POSITIONING metrics for the
semiconductor universe.

Source : polygon_option_bars pob   (option daily bar → volume + "right")   (Bronze)
         JOIN silver_option_greeks sog                                     (Silver)
             ON sog.option_ticker = pob.option_ticker AND sog.ts = pob.ts
Target : silver_option_positioning                                         (Silver)
Grain  : one row per (underlying, ts / trade_date), rebuilt INSERT OR REPLACE.

Everything is computed in a single DuckDB pass:
  * call_volume / put_volume / total_volume  — FILTERed SUM(volume) by "right"
  * put_call_ratio  = put_volume / NULLIF(call_volume, 0)
  * n_contracts     = COUNT(DISTINCT option_ticker)
  * atm_iv          = implied_vol of the row nearest moneyness = 1 that day
                      (over rows with implied_vol / moneyness NOT NULL)
  * call_iv_25d     = AVG(IV) over ~25-delta calls (delta in [0.15, 0.35])
  * put_iv_25d      = AVG(IV) over ~25-delta puts  (delta in [-0.35, -0.15])
  * iv_skew_25d     = put_iv_25d - call_iv_25d

Until the options bronze + greeks layers land, the joined source is empty; the
module logs an ok / 0-row run and returns gracefully (mirrors
etl/silver_option_greeks.py).

Usage:
    python -m etl.silver_option_positioning
    python -m etl.silver_option_positioning --universe config/semi_universe.json
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
    Build the single INSERT ... SELECT that computes the positioning metrics.

    `placeholders` is a comma-separated string of '?' for the underlying IN-list.
    The join in `j` pairs each option daily bar (volume, "right") with its solved
    greeks (implied_vol, delta, moneyness). `atm` picks the nearest-to-ATM contract
    per (underlying, ts) via a QUALIFY window; `agg` computes the volume + skew
    aggregates; the final SELECT derives put_call_ratio and iv_skew_25d.
    """
    return f"""
        INSERT OR REPLACE INTO silver_option_positioning
            (underlying, ts, trade_date, total_volume, call_volume, put_volume,
             put_call_ratio, atm_iv, call_iv_25d, put_iv_25d, iv_skew_25d,
             n_contracts)
        WITH j AS (
            -- LEFT JOIN: volume/contract counts cover ALL day option bars;
            -- greeks-derived fields (iv/delta/moneyness) are NULL where a
            -- contract has no solved greeks row, so they simply don't
            -- contribute to the IV aggregates.
            SELECT
                pob.underlying              AS underlying,
                pob.ts                      AS ts,
                CAST(pob.ts AS DATE)        AS trade_date,
                pob.option_ticker           AS option_ticker,
                CAST(pob.volume AS DOUBLE)  AS volume,
                pob."right"                 AS "right",
                sog.implied_vol             AS implied_vol,
                sog.delta                   AS delta,
                sog.moneyness               AS moneyness
            FROM polygon_option_bars pob
            LEFT JOIN silver_option_greeks sog
              ON sog.option_ticker = pob.option_ticker
             AND sog.ts            = pob.ts
            WHERE pob.timespan = 'day'
              AND pob.underlying IN ({placeholders})
        ),
        atm AS (
            SELECT
                underlying,
                ts,
                implied_vol AS atm_iv
            FROM j
            WHERE implied_vol IS NOT NULL
              AND moneyness   IS NOT NULL
            QUALIFY row_number() OVER (
                PARTITION BY underlying, ts
                ORDER BY ABS(moneyness - 1) ASC, option_ticker
            ) = 1
        ),
        agg AS (
            SELECT
                underlying,
                ts,
                trade_date,
                SUM(volume)                                    AS total_volume,
                SUM(volume) FILTER (WHERE "right" = 'call')    AS call_volume,
                SUM(volume) FILTER (WHERE "right" = 'put')     AS put_volume,
                COUNT(DISTINCT option_ticker)                  AS n_contracts,
                AVG(implied_vol) FILTER (
                    WHERE "right" = 'call' AND delta BETWEEN 0.15 AND 0.35
                )                                              AS call_iv_25d,
                AVG(implied_vol) FILTER (
                    WHERE "right" = 'put' AND delta BETWEEN -0.35 AND -0.15
                )                                              AS put_iv_25d
            FROM j
            GROUP BY underlying, ts, trade_date
        )
        SELECT
            agg.underlying,
            agg.ts,
            agg.trade_date,
            agg.total_volume,
            agg.call_volume,
            agg.put_volume,
            agg.put_volume / NULLIF(agg.call_volume, 0)  AS put_call_ratio,
            atm.atm_iv,
            agg.call_iv_25d,
            agg.put_iv_25d,
            agg.put_iv_25d - agg.call_iv_25d             AS iv_skew_25d,
            agg.n_contracts
        FROM agg
        LEFT JOIN atm
          ON atm.underlying = agg.underlying
         AND atm.ts         = agg.ts
    """


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Silver-layer per-underlying daily options positioning "
                    "metrics for the semiconductor universe."
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
        _log_etl_run("silver_option_positioning", "ok", message, 0,
                     started_at, finished_at)
        print(f"[silver_option_positioning] {message}")
        return

    placeholders = ", ".join(["?"] * len(tickers))
    try:
        with get_connection() as conn:
            conn.execute(_build_sql(placeholders), tickers)
            rows = conn.execute(
                f"SELECT COUNT(*) FROM silver_option_positioning "
                f"WHERE underlying IN ({placeholders})",
                tickers,
            ).fetchone()[0]
            conn.commit()

        finished_at = _utcnow()
        message = f"{rows} underlying-day rows across {len(tickers)} underlyings from {args.universe}"
        _log_etl_run("silver_option_positioning", "ok", message, rows,
                     started_at, finished_at)
        summary = (
            f"[silver_option_positioning] OK — {rows:,} rows across "
            f"{len(tickers)} underlyings"
        )
        logger.info(summary)
        print(summary)
    except Exception as e:
        finished_at = _utcnow()
        message = f"FAILED: {e} | {len(tickers)} underlyings from {args.universe}"
        _log_etl_run("silver_option_positioning", "error", message, 0,
                     started_at, finished_at)
        logger.error(message)
        print(f"[silver_option_positioning] ERROR — {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
