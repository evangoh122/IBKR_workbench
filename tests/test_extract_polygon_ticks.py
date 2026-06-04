"""
tests/test_extract_polygon_ticks.py
Tests for the Polygon ticks ETL extractor.
"""
import pytest
from unittest.mock import MagicMock, patch
from etl.extract_polygon_ticks import run_polygon_ticks_etl
from db.database import get_connection

@pytest.fixture
def mock_polygon_trade():
    trade = MagicMock()
    trade.sip_timestamp = 1717459200000000000  # 2024-06-04
    trade.price = 100.5
    trade.size = 10
    trade.conditions = [1, 2]
    trade.exchange = 4
    trade.tape = "A"
    return trade

def test_run_polygon_ticks_etl_success(tmp_db, mock_polygon_trade):
    client = MagicMock()
    client.list_trades.return_value = [mock_polygon_trade]
    
    tickers = [{"symbol": "AAPL", "secType": "STK"}]
    
    with patch("etl.extract_polygon_ticks._delay", return_value=0):
        count = run_polygon_ticks_etl(client, tickers, "2024-06-04", "2024-06-04", max_per_ticker=1)
        assert count == 1
        
    with get_connection() as conn:
        res = conn.execute("SELECT * FROM polygon_trades").fetchone()
        assert res[0] == "AAPL"
        assert res[2] == 100.5
        assert res[3] == 10
        assert res[4] == "1,2"

def test_run_polygon_ticks_etl_unauthorized(tmp_db):
    client = MagicMock()
    client.list_trades.side_effect = Exception("NOT_AUTHORIZED")
    
    tickers = [{"symbol": "AAPL", "secType": "STK"}]
    
    with patch("etl.extract_polygon_ticks._delay", return_value=0):
        count = run_polygon_ticks_etl(client, tickers, "2024-06-04", "2024-06-04")
        assert count == 0
