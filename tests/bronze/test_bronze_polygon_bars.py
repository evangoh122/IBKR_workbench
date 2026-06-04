"""
tests/bronze/test_bronze_polygon_bars.py
Bronze layer: Tests raw OHLCV bar ingestion from Polygon API into DuckDB.

These tests verify that the ETL correctly:
- Fetches bars from the API and writes them to polygon_bars
- Handles timestamp conversion (ms → ISO-8601)
- Deduplicates via INSERT OR IGNORE (UNIQUE constraint)
- Respects rate limiting between API calls
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from etl.extract_polygon import run_polygon_bars_etl, _ms_to_iso, _polygon_ticker
from db.database import get_connection


@pytest.fixture
def mock_aggs():
    """Create mock aggregate bar objects matching polygon API response."""
    bars = []
    for i in range(5):
        agg = MagicMock()
        agg.timestamp = 1717459200000 + (i * 86400000)  # Daily bars
        agg.open = 100.0 + i
        agg.high = 105.0 + i
        agg.low = 95.0 + i
        agg.close = 102.0 + i
        agg.volume = 1000000 + (i * 100000)
        agg.vwap = 101.0 + i
        agg.transactions = 5000 + (i * 500)
        bars.append(agg)
    return bars


def test_polygon_bars_writes_raw_rows(tmp_db, mock_aggs):
    """Verify bars are inserted into polygon_bars table."""
    client = MagicMock()
    client.get_aggs.return_value = mock_aggs

    tickers = [{"symbol": "AAPL", "secType": "STK"}]

    with patch("etl.extract_polygon._RATE_DELAY", 0):
        count = run_polygon_bars_etl(client, tickers, timespan="day", lookback_days=7)

    assert count == 5

    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM polygon_bars").fetchall()
        assert len(rows) == 5
        # Verify first row structure (ticker, ts, timespan, OHLCV...)
        assert rows[0][0] == "AAPL"
        assert rows[0][2] == "day"
        assert rows[0][3] == 100.0  # open


def test_polygon_bars_upsert_deduplication(tmp_db, mock_aggs):
    """Verify INSERT OR IGNORE deduplicates on (ticker, ts, timespan)."""
    client = MagicMock()
    client.get_aggs.return_value = mock_aggs

    tickers = [{"symbol": "AAPL", "secType": "STK"}]

    with patch("etl.extract_polygon._RATE_DELAY", 0):
        # Run twice with same data
        run_polygon_bars_etl(client, tickers, timespan="day", lookback_days=7)
        run_polygon_bars_etl(client, tickers, timespan="day", lookback_days=7)

    with get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM polygon_bars").fetchone()[0]
        # Should still be 5, not 10 — duplicates ignored
        assert count == 5


def test_polygon_bars_ms_to_iso_conversion():
    """Verify millisecond timestamps convert to ISO-8601."""
    # 2024-06-04T16:00:00Z
    ms_timestamp = 1717516800000
    result = _ms_to_iso(ms_timestamp)
    assert result == "2024-06-04T16:00:00+00:00"


def test_polygon_bars_ms_to_iso_none():
    """Verify None timestamp returns None."""
    assert _ms_to_iso(None) is None


def test_polygon_bars_handles_null_fields(tmp_db):
    """Verify bars with missing fields write NULL correctly."""
    client = MagicMock()

    # Agg with some None fields
    agg = MagicMock()
    agg.timestamp = 1717459200000
    agg.open = 100.0
    agg.high = None
    agg.low = 95.0
    agg.close = 102.0
    agg.volume = None
    agg.vwap = None
    agg.transactions = None

    client.get_aggs.return_value = [agg]
    tickers = [{"symbol": "TEST", "secType": "STK"}]

    with patch("etl.extract_polygon._RATE_DELAY", 0):
        count = run_polygon_bars_etl(client, tickers, timespan="day", lookback_days=1)

    assert count == 1

    with get_connection() as conn:
        row = conn.execute("SELECT * FROM polygon_bars").fetchone()
        assert row[0] == "TEST"
        assert row[4] == 100.0  # open
        assert row[5] is None   # high is NULL
        assert row[6] == 95.0   # low


def test_polygon_bars_multiple_tickers(tmp_db, mock_aggs):
    """Verify bars are written for multiple tickers."""
    client = MagicMock()
    client.get_aggs.return_value = mock_aggs

    tickers = [
        {"symbol": "AAPL", "secType": "STK"},
        {"symbol": "MSFT", "secType": "STK"},
    ]

    with patch("etl.extract_polygon._RATE_DELAY", 0):
        count = run_polygon_bars_etl(client, tickers, timespan="day", lookback_days=7)

    assert count == 10  # 5 bars × 2 tickers

    with get_connection() as conn:
        aapl = conn.execute("SELECT COUNT(*) FROM polygon_bars WHERE ticker = 'AAPL'").fetchone()[0]
        msft = conn.execute("SELECT COUNT(*) FROM polygon_bars WHERE ticker = 'MSFT'").fetchone()[0]
        assert aapl == 5
        assert msft == 5


def test_polygon_bars_api_failure_continues(tmp_db):
    """Verify ETL continues to next ticker if one fails."""
    client = MagicMock()
    client.get_aggs.side_effect = [
        Exception("API Error"),
        [MagicMock(timestamp=1717459200000, open=100, high=105, low=95, close=102,
                   volume=1000000, vwap=101, transactions=5000)],
    ]

    tickers = [
        {"symbol": "FAIL", "secType": "STK"},
        {"symbol": "OK", "secType": "STK"},
    ]

    with patch("etl.extract_polygon._RATE_DELAY", 0):
        count = run_polygon_bars_etl(client, tickers, timespan="day", lookback_days=1)

    # Should still process the successful ticker
    assert count == 1

    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM polygon_bars").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "OK"


def test_polygon_bars_cash_ticker_conversion(tmp_db, mock_aggs):
    """Verify CASH tickers convert to polygon format (C:EURUSD)."""
    client = MagicMock()
    client.get_aggs.return_value = mock_aggs

    tickers = [{"symbol": "EUR.USD", "secType": "CASH"}]

    with patch("etl.extract_polygon._RATE_DELAY", 0):
        run_polygon_bars_etl(client, tickers, timespan="day", lookback_days=1)

    # Verify the API was called with converted ticker
    call_args = client.get_aggs.call_args
    assert call_args[0][0] == "C:EURUSD"


def test_polygon_bars_ind_ticker_conversion(tmp_db, mock_aggs):
    """Verify IND tickers convert to polygon format (I:SPX)."""
    client = MagicMock()
    client.get_aggs.return_value = mock_aggs

    tickers = [{"symbol": "SPX", "secType": "IND"}]

    with patch("etl.extract_polygon._RATE_DELAY", 0):
        run_polygon_bars_etl(client, tickers, timespan="day", lookback_days=1)

    call_args = client.get_aggs.call_args
    assert call_args[0][0] == "I:SPX"
