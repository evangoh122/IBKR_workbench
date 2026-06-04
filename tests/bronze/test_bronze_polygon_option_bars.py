"""
tests/bronze/test_bronze_polygon_option_bars.py
Bronze layer: Tests raw option OHLCV bar ingestion from Polygon API.

These tests verify that the option bars ETL:
- Lists contracts from polygon, then fetches bars per contract
- Writes to polygon_option_bars with correct schema
- Handles NOT_AUTHORIZED for paid-plan endpoints
- Respects max_contracts cap
"""
import pytest
from unittest.mock import MagicMock, patch

from etl.extract_polygon import run_polygon_option_bars_etl
from db.database import get_connection


@pytest.fixture
def mock_contracts():
    """Create mock option contract objects."""
    contracts = []
    for i in range(3):
        c = MagicMock()
        c.ticker = f"O:AAPL240119C0015000{i}"
        c.expiration_date = "2024-01-19"
        c.strike_price = 150.0 + (i * 10)
        c.contract_type = "call"
        contracts.append(c)
    return contracts


@pytest.fixture
def mock_option_aggs():
    """Create mock aggregate bar objects for options."""
    bars = []
    for i in range(5):
        agg = MagicMock()
        agg.timestamp = 1705276800000 + (i * 86400000)  # Daily bars
        agg.open = 5.0 + i
        agg.high = 6.0 + i
        agg.low = 4.0 + i
        agg.close = 5.5 + i
        agg.volume = 10000 + (i * 1000)
        agg.vwap = 5.25 + i
        agg.transactions = 500 + (i * 50)
        bars.append(agg)
    return bars


def test_option_bars_writes_rows(tmp_db, mock_contracts, mock_option_aggs):
    """Verify option bars are inserted into polygon_option_bars."""
    client = MagicMock()
    client.list_options_contracts.return_value = mock_contracts
    client.get_aggs.return_value = mock_option_aggs

    tickers = [{"symbol": "AAPL", "secType": "STK"}]

    with patch("etl.extract_polygon._RATE_DELAY", 0):
        count = run_polygon_option_bars_etl(client, tickers, timespan="day", lookback_days=7)

    # 3 contracts × 5 bars = 15 rows
    assert count == 15

    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM polygon_option_bars").fetchall()
        assert len(rows) == 15
        # Verify first row structure
        assert rows[0][0] == "O:AAPL240119C00150000"  # option_ticker
        assert rows[0][1] == "AAPL"                     # underlying
        assert rows[0][4] == "call"                     # right


def test_option_bars_unauthorized_aborts(tmp_db):
    """Verify ETL returns 0 and aborts on NOT_AUTHORIZED error."""
    client = MagicMock()
    client.list_options_contracts.side_effect = Exception("NOT_AUTHORIZED")

    tickers = [{"symbol": "AAPL", "secType": "STK"}]

    with patch("etl.extract_polygon._RATE_DELAY", 0):
        count = run_polygon_option_bars_etl(client, tickers)

    assert count == 0


def test_option_bars_max_contracts_cap(tmp_db, mock_contracts, mock_option_aggs):
    """Verify max_contracts limits contracts processed."""
    client = MagicMock()
    client.list_options_contracts.return_value = mock_contracts
    client.get_aggs.return_value = mock_option_aggs

    tickers = [{"symbol": "AAPL", "secType": "STK"}]

    with patch("etl.extract_polygon._RATE_DELAY", 0):
        # Set max_contracts=2, but mock returns 3
        count = run_polygon_option_bars_etl(
            client, tickers, timespan="day", lookback_days=7, max_contracts=2
        )

    # Only 2 contracts should be processed (2 × 5 = 10 rows)
    assert count == 10


def test_option_bars_empty_contracts(tmp_db):
    """Verify ETL handles when no contracts are returned."""
    client = MagicMock()
    client.list_options_contracts.return_value = []

    tickers = [{"symbol": "AAPL", "secType": "STK"}]

    with patch("etl.extract_polygon._RATE_DELAY", 0):
        count = run_polygon_option_bars_etl(client, tickers)

    assert count == 0


def test_option_bars_skips_cash(tmp_db):
    """Verify ETL skips CASH (forex) tickers."""
    client = MagicMock()
    tickers = [{"symbol": "EUR.USD", "secType": "CASH"}]

    with patch("etl.extract_polygon._RATE_DELAY", 0):
        count = run_polygon_option_bars_etl(client, tickers)

    assert count == 0
    # API should not be called
    client.list_options_contracts.assert_not_called()


def test_option_bars_deduplication(tmp_db, mock_contracts, mock_option_aggs):
    """Verify INSERT OR REPLACE deduplicates on (option_ticker, ts, timespan)."""
    client = MagicMock()
    client.list_options_contracts.return_value = mock_contracts
    client.get_aggs.return_value = mock_option_aggs

    tickers = [{"symbol": "AAPL", "secType": "STK"}]

    with patch("etl.extract_polygon._RATE_DELAY", 0):
        run_polygon_option_bars_etl(client, tickers, timespan="day", lookback_days=7)
        run_polygon_option_bars_etl(client, tickers, timespan="day", lookback_days=7)

    with get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM polygon_option_bars").fetchone()[0]
        # Should still be 15, not 30 — REPLACE deduplicates
        assert count == 15


def test_option_bars_stores_contract_metadata(tmp_db, mock_contracts, mock_option_aggs):
    """Verify contract metadata (expiry, strike, right) is stored correctly."""
    client = MagicMock()
    client.list_options_contracts.return_value = mock_contracts
    client.get_aggs.return_value = mock_option_aggs

    tickers = [{"symbol": "AAPL", "secType": "STK"}]

    with patch("etl.extract_polygon._RATE_DELAY", 0):
        run_polygon_option_bars_etl(client, tickers, timespan="day", lookback_days=7)

    with get_connection() as conn:
        # Check first contract's metadata
        row = conn.execute(
            'SELECT underlying, expiry, strike, "right" FROM polygon_option_bars LIMIT 1'
        ).fetchone()
        assert row[0] == "AAPL"
        assert row[1] == "2024-01-19"
        assert row[2] == 150.0
        assert row[3] == "call"


def test_option_bars_override_tickers(tmp_db, mock_contracts, mock_option_aggs, monkeypatch):
    """Verify POLYGON_OPTION_BARS_TICKERS env var filters tickers."""
    client = MagicMock()
    client.list_options_contracts.return_value = mock_contracts
    client.get_aggs.return_value = mock_option_aggs

    # Only process MSFT, not AAPL
    monkeypatch.setenv("POLYGON_OPTION_BARS_TICKERS", "MSFT")

    tickers = [
        {"symbol": "AAPL", "secType": "STK"},
        {"symbol": "MSFT", "secType": "STK"},
    ]

    with patch("etl.extract_polygon._RATE_DELAY", 0):
        count = run_polygon_option_bars_etl(client, tickers, timespan="day", lookback_days=7)

    # Only MSFT should be processed
    assert count == 15

    with get_connection() as conn:
        underlying = conn.execute(
            "SELECT DISTINCT underlying FROM polygon_option_bars"
        ).fetchone()[0]
        assert underlying == "MSFT"
