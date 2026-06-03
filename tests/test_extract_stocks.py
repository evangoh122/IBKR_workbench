"""
tests/test_extract_stocks.py
Tests for stock quote ETL.
"""
import threading
import pytest
from unittest.mock import MagicMock
from db.database import get_connection
from etl.extract_stocks import run_stock_etl


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_mock_client(snapshots: dict):
    """
    snapshots = {ticker: {field: val}}
    Simulates request_snapshot callback.
    """
    req_counter = [0]
    def fake_snapshot(contract, on_done):
        req_id = req_counter[0]
        req_counter[0] += 1
        snap = snapshots.get(contract.symbol, {})
        # Call the callback immediately or in a thread
        t = threading.Thread(target=on_done, args=(req_id, snap))
        t.start()
        return req_id

    client = MagicMock()
    client.make_contract.side_effect = lambda **kw: MagicMock(symbol=kw.get("symbol"))
    client.request_snapshot.side_effect = fake_snapshot
    return client


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_run_stock_etl_writes_rows(tmp_db):
    sample_tickers = [
        {"symbol": "AAPL", "secType": "STK", "exchange": "SMART", "currency": "USD"},
        {"symbol": "MSFT", "secType": "STK", "exchange": "SMART", "currency": "USD"},
        {"symbol": "SPY", "secType": "STK", "exchange": "SMART", "currency": "USD"}
    ]
    snapshots = {
        "AAPL": {"ts": "2024-01-15T14:30:00+00:00", "bid": 182.5, "ask": 182.6,
                 "last": 182.55, "close": 181.0, "volume": 50_000_000,
                 "open": 180.0, "high": 183.0, "low": 179.0},
        "MSFT": {"ts": "2024-01-15T14:30:00+00:00", "bid": 374.0, "ask": 374.2,
                 "last": 374.1, "close": 372.0, "volume": 20_000_000,
                 "open": 371.0, "high": 375.0, "low": 370.0},
        "SPY":  {"ts": "2024-01-15T14:30:00+00:00", "bid": 470.0, "ask": 470.1,
                 "last": 470.05, "close": 469.0, "volume": 80_000_000,
                 "open": 468.0, "high": 471.0, "low": 467.0},
    }
    client = _make_mock_client(snapshots)
    rows_written = run_stock_etl(client, sample_tickers)

    assert rows_written == 3

    with get_connection() as conn:
        df = conn.execute("SELECT * FROM stock_quotes ORDER BY ticker").df()

    assert len(df) == 3
    tickers = df["ticker"].tolist()
    assert sorted(tickers) == ["AAPL", "MSFT", "SPY"]


def test_run_stock_etl_correct_values(tmp_db):
    snapshots = {
        "AAPL": {"ts": "2024-01-15T14:30:00+00:00",
                 "bid": 182.5, "ask": 182.6, "last": 182.55,
                 "close": 181.0, "volume": 50_000_000,
                 "open": 180.0, "high": 183.0, "low": 179.0},
    }
    client = _make_mock_client(snapshots)
    run_stock_etl(client, [{"symbol": "AAPL", "secType": "STK", "exchange": "SMART", "currency": "USD"}])

    with get_connection() as conn:
        df = conn.execute("SELECT * FROM stock_quotes WHERE ticker='AAPL'").df()
    row = df.iloc[0]

    assert row["bid"]    == 182.5
    assert row["ask"]    == 182.6
    assert row["last"]   == 182.55
    assert row["volume"] == 50_000_000


def test_run_stock_etl_empty_snapshot(tmp_db):
    """If client returns empty dict, 0 rows should be written."""
    client = _make_mock_client({})
    rows = run_stock_etl(client, [{"symbol": "AAPL", "secType": "STK", "exchange": "SMART", "currency": "USD"}])
    assert rows == 0


def test_run_stock_etl_partial_snapshots(tmp_db):
    """Should handle snapshots with missing fields gracefully."""
    snapshots = {
        "AAPL": {"ts": "2024-01-15T14:30:00+00:00", "last": 182.55}
    }
    client = _make_mock_client(snapshots)
    run_stock_etl(client, [{"symbol": "AAPL", "secType": "STK", "exchange": "SMART", "currency": "USD"}])

    with get_connection() as conn:
        df = conn.execute("SELECT * FROM stock_quotes WHERE ticker='AAPL'").df()
    row = df.iloc[0]
    assert row["last"] == 182.55
    import pandas as pd
    assert pd.isna(row["bid"])
