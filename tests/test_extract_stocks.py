"""
tests/test_extract_stocks.py
Tests for stock snapshot extraction and DB writes.
"""
import threading
import pytest
from unittest.mock import MagicMock, patch
from db.database import get_connection
from etl.extract_stocks import run_stock_etl


def _make_mock_client(snapshots: dict):
    """
    Returns a mock IBKRClient whose request_snapshot immediately
    calls on_done with the provided snapshot for each ticker.
    """
    req_counter = [0]

    def fake_request_snapshot(contract, on_done):
        req_id = req_counter[0]
        req_counter[0] += 1
        ticker = contract.symbol
        snap = snapshots.get(ticker, {})
        # Fire callback in a thread to mimic async behaviour
        t = threading.Thread(target=on_done, args=(req_id, snap))
        t.start()
        return req_id

    client = MagicMock()
    client.make_stock_contract.side_effect = lambda t: MagicMock(symbol=t)
    client.request_snapshot.side_effect = fake_request_snapshot
    return client


def test_run_stock_etl_writes_rows(sample_tickers, tmp_db):
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

    conn = get_connection()
    rows = conn.execute("SELECT * FROM stock_quotes ORDER BY ticker").fetchall()
    conn.close()

    assert len(rows) == 3
    tickers = [r["ticker"] for r in rows]
    assert "AAPL" in tickers
    assert "MSFT" in tickers
    assert "SPY"  in tickers


def test_run_stock_etl_correct_values(tmp_db):
    snapshots = {
        "AAPL": {"ts": "2024-01-15T14:30:00+00:00",
                 "bid": 182.5, "ask": 182.6, "last": 182.55,
                 "close": 181.0, "volume": 50_000_000,
                 "open": 180.0, "high": 183.0, "low": 179.0},
    }
    client = _make_mock_client(snapshots)
    run_stock_etl(client, ["AAPL"])

    conn = get_connection()
    row = conn.execute("SELECT * FROM stock_quotes WHERE ticker='AAPL'").fetchone()
    conn.close()

    assert row["bid"]    == 182.5
    assert row["ask"]    == 182.6
    assert row["last"]   == 182.55
    assert row["volume"] == 50_000_000
    assert row["high"]   == 183.0
    assert row["low"]    == 179.0


def test_run_stock_etl_empty_snapshot(tmp_db):
    """Empty snapshots should produce 0 rows and not crash."""
    client = _make_mock_client({})
    rows_written = run_stock_etl(client, ["AAPL"])
    assert rows_written == 0

    conn = get_connection()
    count = conn.execute("SELECT COUNT(*) FROM stock_quotes").fetchone()[0]
    conn.close()
    assert count == 0


def test_run_stock_etl_partial_snapshots(tmp_db):
    """Only tickers with data should be written."""
    snapshots = {
        "AAPL": {"ts": "2024-01-15T14:30:00+00:00", "last": 182.5},
        # MSFT returns empty
    }
    client = _make_mock_client(snapshots)
    rows_written = run_stock_etl(client, ["AAPL", "MSFT"])
    assert rows_written == 1
