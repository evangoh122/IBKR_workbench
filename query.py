"""
query.py
Convenience functions for querying the IBKR ETL database.
Run as a script for a quick CLI summary:
    python query.py
"""
import os
import duckdb
from datetime import datetime, timezone, timedelta

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "./data/ibkr.duckdb")


def _conn():
    return duckdb.connect(DB_PATH)


# ── Stock helpers ─────────────────────────────────────────────────────────────

def latest_stock_quotes() -> pd.DataFrame:
    """Most recent quote for each ticker."""
    with _conn() as conn:
        df = conn.execute("""
            SELECT s.*
            FROM stock_quotes s
            INNER JOIN (
                SELECT ticker, MAX(ts) AS max_ts
                FROM stock_quotes
                GROUP BY ticker
            ) lq ON s.ticker = lq.ticker AND s.ts = lq.max_ts
            ORDER BY s.ticker
        """).df()
    return df


def stock_history(ticker: str, hours: int = 24) -> pd.DataFrame:
    """Price history for a ticker over the last N hours."""
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with _conn() as conn:
        df = conn.execute("""
            SELECT ticker, ts, bid, ask, last, volume, "open", high, low, "close"
            FROM stock_quotes
            WHERE ticker = ? AND ts >= ?
            ORDER BY ts
        """, (ticker, since)).df()
    return df


# ── Options helpers ───────────────────────────────────────────────────────────

def latest_option_quotes(ticker: str, expiry: str = None) -> pd.DataFrame:
    """Most recent option quotes for a ticker (optionally filtered by expiry)."""
    where = "WHERE oq.ticker = ?"
    params = [ticker]
    if expiry:
        where += " AND oq.expiry = ?"
        params.append(expiry)

    with _conn() as conn:
        df = conn.execute(f"""
            SELECT oq.*
            FROM option_quotes oq
            INNER JOIN (
                SELECT ticker, expiry, strike, "right", MAX(ts) AS max_ts
                FROM option_quotes
                GROUP BY ticker, expiry, strike, "right"
            ) lq ON  oq.ticker = lq.ticker
                 AND oq.expiry = lq.expiry
                 AND oq.strike = lq.strike
                 AND oq."right"  = lq."right"
                 AND oq.ts     = lq.max_ts
            {where}
            ORDER BY oq.expiry, oq.strike, oq."right"
        """, params).df()
    return df


def option_chain_summary(ticker: str) -> pd.DataFrame:
    """Available expiries and strike count in the metadata table."""
    with _conn() as conn:
        df = conn.execute("""
            SELECT ticker, expiry,
                   COUNT(DISTINCT strike) AS strikes,
                   SUM(CASE WHEN "right"='C' THEN 1 ELSE 0 END) AS calls,
                   SUM(CASE WHEN "right"='P' THEN 1 ELSE 0 END) AS puts
            FROM option_chains
            WHERE ticker = ?
            GROUP BY ticker, expiry
            ORDER BY expiry
        """, (ticker,)).df()
    return df


def etl_run_log(limit: int = 20) -> pd.DataFrame:
    with _conn() as conn:
        df = conn.execute("""
            SELECT * FROM etl_runs ORDER BY id DESC LIMIT ?
        """, (limit,)).df()
    return df


# ── Vector Search helpers ─────────────────────────────────────────────────────

def search_similar_tickers(query_vector: list, limit: int = 5) -> pd.DataFrame:
    """Find tickers with similar embeddings using DuckDB VSS."""
    # Ensure flat Python list of floats (handles numpy arrays, nested lists, etc.)
    vec = [float(x) for x in query_vector]
    with _conn() as conn:
        df = conn.execute("""
            SELECT ticker, industry, text,
                   array_distance(embedding, ?::FLOAT[384]) as distance
            FROM ticker_embeddings
            ORDER BY distance
            LIMIT ?
        """, [vec, limit]).df()
    return df


# ── CLI summary ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    print("\n=== Latest Stock Quotes ===")
    sq = latest_stock_quotes()
    if sq.empty:
        print("  (no data yet)")
    else:
        print(sq[["ticker","ts","bid","ask","last","volume"]].to_string(index=False))

    print("\n=== Recent ETL Runs ===")
    runs = etl_run_log(10)
    if runs.empty:
        print("  (no runs logged)")
    else:
        print(runs[["run_type","status","rows_written","started_at","message"]]
              .to_string(index=False))
