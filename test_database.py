"""
tests/test_database.py
Tests for DB initialisation, schema, and connection.
"""
import sqlite3
import pytest
from db.database import init_db, get_connection


def test_init_creates_tables(tmp_db):
    conn = sqlite3.connect(tmp_db)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    conn.close()
    assert "stock_quotes"  in tables
    assert "option_quotes" in tables
    assert "option_chains" in tables
    assert "etl_runs"      in tables


def test_init_is_idempotent(tmp_db):
    """Calling init_db twice should not raise."""
    init_db()
    init_db()


def test_stock_quotes_columns(db_conn):
    cols = {r[1] for r in db_conn.execute("PRAGMA table_info(stock_quotes)")}
    expected = {"ticker", "ts", "bid", "ask", "last", "close",
                "volume", "open", "high", "low"}
    assert expected.issubset(cols)


def test_option_quotes_columns(db_conn):
    cols = {r[1] for r in db_conn.execute("PRAGMA table_info(option_quotes)")}
    expected = {"ticker", "expiry", "strike", "right", "bid", "ask",
                "implied_vol", "delta", "gamma", "theta", "vega"}
    assert expected.issubset(cols)


def test_get_connection_returns_row_factory(tmp_db):
    conn = get_connection()
    conn.execute("""
        INSERT INTO stock_quotes (ticker, ts, last)
        VALUES ('TEST', '2024-01-01T00:00:00+00:00', 100.0)
    """)
    conn.commit()
    row = conn.execute("SELECT * FROM stock_quotes LIMIT 1").fetchone()
    assert row["ticker"] == "TEST"
    conn.close()


def test_wal_mode_enabled(tmp_db):
    conn = get_connection()
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    conn.close()
    assert mode == "wal"
