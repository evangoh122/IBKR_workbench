"""
tests/test_database.py
Tests for DuckDB initialisation, schema, and connection.
"""
import duckdb
import pytest
from db.database import init_db, get_connection


def test_init_creates_tables(tmp_db):
    with duckdb.connect(tmp_db) as conn:
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    assert "stock_quotes"  in tables
    assert "option_quotes" in tables
    assert "option_chains" in tables
    assert "etl_runs"      in tables
    assert "cot_reports"   in tables


def test_init_is_idempotent(tmp_db):
    """Calling init_db twice should not raise."""
    init_db()
    init_db()


def test_stock_quotes_columns(db_conn):
    cols = {r[0] for r in db_conn.execute("DESCRIBE stock_quotes").fetchall()}
    expected = {"ticker", "ts", "bid", "ask", "last", "close",
                "volume", "open", "high", "low", "vwap"}
    assert expected.issubset(cols)


def test_option_quotes_columns(db_conn):
    cols = {r[0] for r in db_conn.execute("DESCRIBE option_quotes").fetchall()}
    expected = {"ticker", "expiry", "strike", "right", "bid", "ask",
                "implied_vol", "delta", "gamma", "theta", "vega", "und_price", "pv_dividend"}
    assert expected.issubset(cols)


def test_get_connection_and_fetch_df(tmp_db):
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO stock_quotes (ticker, ts, last)
            VALUES ('TEST', '2024-01-01T00:00:00+00:00', 100.0)
        """)
        conn.commit()
        df = conn.execute("SELECT * FROM stock_quotes LIMIT 1").df()
        assert df.iloc[0]["ticker"] == "TEST"
