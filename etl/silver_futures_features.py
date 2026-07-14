"""
etl/silver_futures_features.py
Silver-layer futures continuous-contract price features (index futures + VIX
term structure).

Source : polygon_bars WHERE timespan='day' AND ticker IN (FUTURES_TICKERS)   (Bronze)
Target : silver_futures_features                                             (Silver)
Grain  : one row per (ticker, trade_date), rebuilt with INSERT OR REPLACE.

VIX2:COM is pulled in as context only (to compute the VIX1/VIX2 term-structure
slope) but does not get its own silver_futures_features row.

Usage:
    python -m etl.silver_futures_features
"""
import logging

import duckdb

logger = logging.getLogger(__name__)

# Continuous front-month/second-month futures contracts (Polygon ':COM' continuous tickers)
FUTURES_TICKERS = ["ES1:COM", "NQ1:COM", "RTY1:COM", "VIX1:COM", "VIX2:COM"]
# Tickers that get a persisted silver_futures_features row
OUTPUT_TICKERS = ["ES1:COM", "NQ1:COM", "RTY1:COM", "VIX1:COM"]


def _build_sql(placeholders: str, output_placeholders: str) -> str:
    return f"""
        INSERT OR REPLACE INTO silver_futures_features
            (trade_date, ticker, open_price, high_price, low_price, close_price, volume,
             n20, ma_20, std_20, zscore_20,
             n_ret20, ret, mean_ret_20, std_ret_20, zscore_ret_20,
             vx_term_slope, regime_flag)
        WITH base AS (
            SELECT
                CAST(ts AS DATE) AS trade_date,
                ticker,
                CAST(open   AS DOUBLE) AS open_price,
                CAST(high   AS DOUBLE) AS high_price,
                CAST(low    AS DOUBLE) AS low_price,
                CAST(close  AS DOUBLE) AS close_price,
                CAST(volume AS BIGINT) AS volume
            FROM polygon_bars
            WHERE timespan = 'day'
              AND ticker IN ({placeholders})
        ),
        rets AS (
            SELECT
                *,
                close_price / NULLIF(LAG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date), 0) - 1
                    AS ret
            FROM base
        ),
        feat AS (
            SELECT
                *,
                COUNT(close_price) OVER w20       AS n20,
                AVG(close_price)   OVER w20       AS ma20_raw,
                STDDEV_SAMP(close_price) OVER w20 AS std20_raw,
                COUNT(ret) OVER w20                AS n_ret20,
                AVG(ret)   OVER w20                AS mean_ret20,
                STDDEV_SAMP(ret) OVER w20           AS std_ret20
            FROM rets
            WINDOW w20 AS (
                PARTITION BY ticker ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            )
        ),
        vix_term AS (
            -- VIX term-structure slope: VIX1/VIX2 - 1 (negative = backwardation = risk-off)
            SELECT
                v1.trade_date,
                v1.close_price / NULLIF(v2.close_price, 0) - 1 AS vx_term_slope
            FROM (SELECT trade_date, close_price FROM base WHERE ticker = 'VIX1:COM') v1
            LEFT JOIN (SELECT trade_date, close_price FROM base WHERE ticker = 'VIX2:COM') v2
                ON v1.trade_date = v2.trade_date
        )
        SELECT
            f.trade_date,
            f.ticker,
            f.open_price,
            f.high_price,
            f.low_price,
            f.close_price,
            f.volume,
            f.n20,
            CASE WHEN f.n20 >= 20 THEN f.ma20_raw ELSE NULL END AS ma_20,
            CASE WHEN f.n20 >= 20 THEN f.std20_raw ELSE NULL END AS std_20,
            CASE WHEN f.n20 >= 20 AND f.std20_raw > 0
                 THEN (f.close_price - f.ma20_raw) / f.std20_raw ELSE NULL
            END AS zscore_20,
            f.n_ret20,
            f.ret,
            CASE WHEN f.n_ret20 >= 20 THEN f.mean_ret20 ELSE NULL END AS mean_ret_20,
            CASE WHEN f.n_ret20 >= 20 THEN f.std_ret20 ELSE NULL END AS std_ret_20,
            CASE WHEN f.n_ret20 >= 20 AND f.std_ret20 > 0
                 THEN (f.ret - f.mean_ret20) / f.std_ret20 ELSE NULL
            END AS zscore_ret_20,
            vt.vx_term_slope,
            CASE
                WHEN f.ticker = 'VIX1:COM' AND vt.vx_term_slope < 0 THEN 'backwardation'
                WHEN f.ticker = 'VIX1:COM' AND vt.vx_term_slope >= 0 THEN 'contango'
                ELSE NULL
            END AS regime_flag
        FROM feat f
        LEFT JOIN vix_term vt ON f.trade_date = vt.trade_date AND f.ticker = 'VIX1:COM'
        WHERE f.ticker IN ({output_placeholders})
        ORDER BY f.ticker, f.trade_date
    """


def run(conn: duckdb.DuckDBPyConnection) -> int:
    """Compute silver futures features for OUTPUT_TICKERS. Returns total row count."""
    placeholders = ", ".join(["?"] * len(FUTURES_TICKERS))
    output_placeholders = ", ".join(["?"] * len(OUTPUT_TICKERS))
    sql = _build_sql(placeholders, output_placeholders)
    conn.execute(sql, FUTURES_TICKERS + OUTPUT_TICKERS)
    result = conn.execute(
        f"SELECT COUNT(*) FROM silver_futures_features WHERE ticker IN ({output_placeholders})",
        OUTPUT_TICKERS,
    ).fetchone()
    rows = result[0] if result else 0
    logger.info("silver_futures_features: %d total rows", rows)
    return rows


def main():
    from db.database import get_connection, init_db

    logging.basicConfig(level=logging.INFO)
    init_db()
    with get_connection() as conn:
        rows = run(conn)
        conn.commit()
    print(f"[silver_futures_features] OK - {rows} total rows")


if __name__ == "__main__":
    main()
