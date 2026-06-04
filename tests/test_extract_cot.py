"""
tests/test_extract_cot.py
Tests for the COT ETL extractor.
"""
import pytest
import requests
from unittest.mock import MagicMock, patch
from etl.extract_cot import run_cot_etl, _to_int
from db.database import get_connection

@pytest.fixture
def mock_cftc_response():
    return [
        {
            "market_and_exchange_names": "GOLD - COMMODITY EXCHANGE INC.",
            "report_date_as_yyyy_mm_dd": "2026-05-26T00:00:00.000",
            "noncomm_positions_long_all": "200,704",
            "noncomm_positions_short_all": "46444",
            "comm_positions_long_all": "74641",
            "comm_positions_short_all": "260407",
            "tot_rept_positions_long_all": "306340",
            "tot_rept_positions_short": "337846",
            "noncomm_postions_spread_all": "30995",
            "open_interest_all": "353489"
        }
    ]

def test_to_int():
    assert _to_int("1,234") == 1234
    assert _to_int("1234.56") == 1234
    assert _to_int(None) is None
    assert _to_int("") is None
    assert _to_int("abc") is None

def test_run_cot_etl_success(tmp_db, mock_cftc_response):
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_cftc_response
        
        count = run_cot_etl(limit=1)
        assert count == 1
        
    with get_connection() as conn:
        res = conn.execute("SELECT * FROM cot_reports").fetchone()
        assert res[0] == "GOLD - COMMODITY EXCHANGE INC."
        assert res[1] == "GC"  # Mapped ticker
        assert res[2] == "2026-05-26"
        assert res[3] == 200704
        assert res[10] == 353489

def test_run_cot_etl_empty(tmp_db):
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = []
        
        count = run_cot_etl()
        assert count == 0

def test_run_cot_etl_error(tmp_db):
    with patch("requests.get") as mock_get:
        mock_get.side_effect = Exception("API error")
        
        count = run_cot_etl()
        assert count == 0
