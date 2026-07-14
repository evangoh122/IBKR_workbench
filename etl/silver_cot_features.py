"""
etl/silver_cot_features.py
Silver-layer COT (Commitments of Traders) positioning features.

Source : cot_reports WHERE ticker IN (COT_UNIVERSE)               (Bronze)
Target : silver_cot_features                                       (Silver)
Grain  : one row per (ticker, report_date), rebuilt with INSERT OR REPLACE.

Rolling 52-week z-scores on net speculator/commercial positioning, following
the same min-periods convention as silver_stock_features.py: a z-score is
only emitted once 26+ weeks of history exist for that ticker (half the
52-week window), otherwise NULL.

Usage:
    python -m etl.silver_cot_features
"""
import logging

import duckdb

logger = logging.getLogger(__name__)

# COT market tickers, mapped to IBKR symbols in cot_reports.ticker by
# etl/extract_cot.py (ES, NQ, RTY, VIX).
COT_UNIVERSE = ["ES", "NQ", "RTY", "VIX"]


def _build_sql(placeholders: str) -> str:
    return f"""
        INSERT OR REPLACE INTO silver_cot_features
            (report_date, ticker, noncomm_long, noncomm_short, noncomm_net,
             comm_long, comm_short, comm_net, n_weeks,
             net_pos_mean_52w, net_pos_std_52w, net_pos_zscore_52w,
             comm_net_mean_52w, comm_net_std_52w, comm_net_zscore_52w,
             spec_comm_divergence, crowd_flag)
        WITH base AS (
            SELECT
                CAST(report_date AS DATE) AS report_date,
                ticker,
                noncomm_long::BIGINT  AS noncomm_long,
                noncomm_short::BIGINT AS noncomm_short,
                (noncomm_long::BIGINT - noncomm_short::BIGINT) AS noncomm_net,
                comm_long::BIGINT  AS comm_long,
                comm_short::BIGINT AS comm_short,
                (comm_long::BIGINT - comm_short::BIGINT) AS comm_net
            FROM cot_reports
            WHERE ticker IN ({placeholders})
        ),
        feat AS (
            SELECT
                report_date,
                ticker,
                noncomm_long,
                noncomm_short,
                noncomm_net,
                comm_long,
                comm_short,
                comm_net,
                COUNT(noncomm_net) OVER w52       AS n_weeks,
                AVG(noncomm_net)   OVER w52       AS net_pos_mean_52w,
                STDDEV_SAMP(noncomm_net) OVER w52 AS net_pos_std_52w,
                AVG(comm_net)      OVER w52       AS comm_net_mean_52w,
                STDDEV_SAMP(comm_net)    OVER w52 AS comm_net_std_52w
            FROM base
            WINDOW w52 AS (
                PARTITION BY ticker
                ORDER BY report_date
                ROWS BETWEEN 51 PRECEDING AND CURRENT ROW
            )
        )
        SELECT
            report_date,
            ticker,
            noncomm_long,
            noncomm_short,
            noncomm_net,
            comm_long,
            comm_short,
            comm_net,
            n_weeks,
            net_pos_mean_52w,
            net_pos_std_52w,
            CASE WHEN n_weeks >= 26 AND net_pos_std_52w > 0
                 THEN (noncomm_net - net_pos_mean_52w) / net_pos_std_52w
                 ELSE NULL
            END AS net_pos_zscore_52w,
            comm_net_mean_52w,
            comm_net_std_52w,
            CASE WHEN n_weeks >= 26 AND comm_net_std_52w > 0
                 THEN (comm_net - comm_net_mean_52w) / comm_net_std_52w
                 ELSE NULL
            END AS comm_net_zscore_52w,
            -- divergence: spec z-score minus commercial z-score
            CASE WHEN n_weeks >= 26 AND net_pos_std_52w > 0 AND comm_net_std_52w > 0
                 THEN ((noncomm_net - net_pos_mean_52w) / net_pos_std_52w)
                      - ((comm_net - comm_net_mean_52w) / comm_net_std_52w)
                 ELSE NULL
            END AS spec_comm_divergence,
            CASE
                WHEN n_weeks >= 26 AND net_pos_std_52w > 0
                     AND (noncomm_net - net_pos_mean_52w) / net_pos_std_52w >= 2.0
                     THEN 'extreme_long'
                WHEN n_weeks >= 26 AND net_pos_std_52w > 0
                     AND (noncomm_net - net_pos_mean_52w) / net_pos_std_52w <= -2.0
                     THEN 'extreme_short'
                WHEN n_weeks >= 26
                     THEN 'neutral'
                ELSE NULL
            END AS crowd_flag
        FROM feat
        ORDER BY ticker, report_date
    """


def run(conn: duckdb.DuckDBPyConnection) -> int:
    """Compute silver COT features for COT_UNIVERSE. Returns total row count."""
    placeholders = ", ".join(["?"] * len(COT_UNIVERSE))
    conn.execute(_build_sql(placeholders), COT_UNIVERSE)
    result = conn.execute("SELECT COUNT(*) FROM silver_cot_features").fetchone()
    rows = result[0] if result else 0
    logger.info("silver_cot_features: %d total rows", rows)
    return rows


def main():
    from db.database import get_connection, init_db

    logging.basicConfig(level=logging.INFO)
    init_db()
    with get_connection() as conn:
        rows = run(conn)
        conn.commit()
    print(f"[silver_cot_features] OK - {rows} total rows")


if __name__ == "__main__":
    main()
