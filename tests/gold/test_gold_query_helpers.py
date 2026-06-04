"""
tests/gold/test_gold_query_helpers.py
Gold layer: Tests for query helper functions that consume OHLCV data.

These tests verify that:
- Query functions return correct data from seeded tables
- Time-based filters work correctly
- Aggregation logic is accurate
- DataFrames have expected columns and types
"""
import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta

from db.database import get_connection
from query import (
    stock_history,
    latest_stock_quotes,
    latest_option_quotes,
    option_chain_summary,
)


@pytest.fixture
def seed_stock_quotes(tmp_db, db_conn):
    """Seed stock_quotes with historical OHLCV data."""
    now = datetime.now(timezone.utc)
    rows = []
    for i in range(48):  # 48 hours of data
        ts = (now - timedelta(hours=i)).isoformat()
        rows.append((
            "AAPL", ts,
            100.0 + i * 0.5,  # bid
            101.0 + i * 0.5,  # ask
            100.5 + i * 0.5,  # last
            100.0 + i * 0.5,  # close
            1000000 - i * 10000,  # volume
            99.0 + i * 0.5,   # open
            105.0 + i * 0.5,  # high
            95.0 + i * 0.5,   # low
            100.25 + i * 0.5, # vwap
        ))

    db_conn.executemany("""
        INSERT INTO stock_quotes (ticker, ts, bid, ask, last, "close", volume, "open", high, low, vwap)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    db_conn.commit()
    return tmp_db


@pytest.fixture
def seed_option_quotes(tmp_db, db_conn):
    """Seed option_quotes with test data."""
    now = datetime.now(timezone.utc)
    rows = []
    for i in range(5):
        ts = (now - timedelta(hours=i)).isoformat()
        rows.append((
            "AAPL", "2024-06-21", 150.0 + i * 5, "C", ts,
            5.0 + i, 5.5 + i, 5.25 + i,
            1000 - i * 100,  # volume
            500 - i * 50,    # open_interest
            0.25 + i * 0.01, # implied_vol
            0.5, 0.02, -0.01, 0.1,  # delta, gamma, theta, vega
            155.0, 1.2,  # und_price, pv_dividend
        ))

    db_conn.executemany("""
        INSERT INTO option_quotes
            (ticker, expiry, strike, "right", ts, bid, ask, last,
             volume, open_interest, implied_vol, delta, gamma, theta, vega,
             und_price, pv_dividend)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    db_conn.commit()
    return tmp_db


def test_stock_history_returns_dataframe(seed_stock_quotes, monkeypatch):
    """Verify stock_history returns a DataFrame with expected columns."""
    monkeypatch.setattr("query.DB_PATH", seed_stock_quotes)

    df = stock_history("AAPL", hours=24)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "ticker" in df.columns
    assert "ts" in df.columns
    assert "bid" in df.columns
    assert "ask" in df.columns
    assert "last" in df.columns


def test_stock_history_time_filter(seed_stock_quotes, monkeypatch):
    """Verify stock_history filters by time window."""
    monkeypatch.setattr("query.DB_PATH", seed_stock_quotes)

    df_24h = stock_history("AAPL", hours=24)
    df_1h = stock_history("AAPL", hours=1)

    # 1h should have fewer rows than 24h
    assert len(df_1h) < len(df_24h)
    # 1h should have ~1 row (current hour only)
    assert len(df_1h) <= 2


def test_stock_history_ticker_filter(seed_stock_quotes, monkeypatch):
    """Verify stock_history only returns data for requested ticker."""
    monkeypatch.setattr("query.DB_PATH", seed_stock_quotes)

    df = stock_history("AAPL", hours=48)

    # Should only have AAPL rows
    assert (df["ticker"] == "AAPL").all()


def test_stock_history_empty_for_unknown_ticker(seed_stock_quotes, monkeypatch):
    """Verify stock_history returns empty DataFrame for unknown ticker."""
    monkeypatch.setattr("query.DB_PATH", seed_stock_quotes)

    df = stock_history("UNKNOWN", hours=24)

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_latest_stock_quotes_returns_most_recent(seed_stock_quotes, monkeypatch):
    """Verify latest_stock_quotes returns the most recent quote per ticker."""
    monkeypatch.setattr("query.DB_PATH", seed_stock_quotes)

    df = latest_stock_quotes()

    assert not df.empty
    assert len(df) == 1  # Only AAPL
    # Should have the most recent timestamp
    assert df.iloc[0]["ticker"] == "AAPL"


def test_latest_option_quotes_by_expiry(seed_option_quotes, monkeypatch):
    """Verify latest_option_quotes filters by expiry."""
    monkeypatch.setattr("query.DB_PATH", seed_option_quotes)

    df = latest_option_quotes("AAPL", expiry="2024-06-21")

    assert not df.empty
    # All rows should have the requested expiry
    assert (df["expiry"] == "2024-06-21").all()


def test_latest_option_quotes_all_expiries(seed_option_quotes, monkeypatch):
    """Verify latest_option_quotes returns all expiries when not filtered."""
    monkeypatch.setattr("query.DB_PATH", seed_option_quotes)

    df = latest_option_quotes("AAPL")

    assert not df.empty
    # Should have data for the ticker
    assert (df["ticker"] == "AAPL").all()


def test_latest_option_quotes_empty_for_unknown(seed_option_quotes, monkeypatch):
    """Verify latest_option_quotes returns empty for unknown ticker."""
    monkeypatch.setattr("query.DB_PATH", seed_option_quotes)

    df = latest_option_quotes("UNKNOWN")

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_option_chain_summary_aggregation(tmp_db, db_conn, monkeypatch):
    """Verify option_chain_summary aggregates correctly."""
    # Seed option_chains
    chains = [
        ("AAPL", "2024-06-21", 150.0, "C", "NYSE"),
        ("AAPL", "2024-06-21", 150.0, "P", "NYSE"),
        ("AAPL", "2024-06-21", 155.0, "C", "NYSE"),
        ("AAPL", "2024-06-28", 150.0, "C", "NYSE"),
    ]
    db_conn.executemany("""
        INSERT INTO option_chains (ticker, expiry, strike, "right", exchange)
        VALUES (?, ?, ?, ?, ?)
    """, chains)
    db_conn.commit()

    monkeypatch.setattr("query.DB_PATH", tmp_db)

    df = option_chain_summary("AAPL")

    assert not df.empty
    # Should have 2 expiry groups
    assert len(df) == 2

    # June 21 should have 2 strikes, 2 calls, 1 put
    june21 = df[df["expiry"] == "2024-06-21"]
    assert june21.iloc[0]["strikes"] == 2
    assert june21.iloc[0]["calls"] == 2
    assert june21.iloc[0]["puts"] == 1


def test_option_chain_summary_empty_for_unknown(tmp_db, db_conn, monkeypatch):
    """Verify option_chain_summary returns empty for unknown ticker."""
    monkeypatch.setattr("query.DB_PATH", tmp_db)

    df = option_chain_summary("UNKNOWN")

    assert isinstance(df, pd.DataFrame)
    assert df.empty
