"""
tests/silver/test_silver_conversions.py
Silver layer: Tests for data type conversions and transformations.

These tests verify that:
- Millisecond timestamps convert correctly to ISO-8601
- Ticker formats convert properly (STK, CASH, IND, FUT)
- Edge cases are handled (None, empty strings, invalid formats)

Note: We test the conversion logic directly rather than importing from
etl.extract_polygon to avoid polygon-api-client version issues in CI.
"""
import pytest
from datetime import datetime, timezone


def _ms_to_iso(ms):
    """Convert millisecond timestamp to ISO-8601 string. Copied from extract_polygon."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="seconds")


def _polygon_ticker(t_def):
    """Convert IBKR ticker dict to polygon format. Copied from extract_polygon."""
    if not isinstance(t_def, dict):
        return str(t_def).strip().replace(" ", ".")

    sec_type = t_def.get("secType", "STK")
    symbol = t_def.get("symbol", "").strip()

    if sec_type == "CASH":
        return "C:" + symbol.replace(".", "")
    if sec_type == "IND":
        return "I:" + symbol
    if sec_type == "FUT":
        return "F:" + symbol
    return symbol.replace(" ", ".")


def _nested(obj, attr):
    """Safely get obj.attr. Copied from extract_polygon."""
    if obj is None:
        return None
    return getattr(obj, attr, None)


class TestMsToIsoConversion:
    """Tests for _ms_to_iso timestamp conversion."""

    def test_valid_timestamp(self):
        """Standard millisecond timestamp converts to ISO-8601."""
        result = _ms_to_iso(1717516800000)
        assert result == "2024-06-04T16:00:00+00:00"

    def test_epoch_zero(self):
        """Unix epoch (0ms) converts to 1970-01-01T00:00:00+00:00."""
        result = _ms_to_iso(0)
        assert result == "1970-01-01T00:00:00+00:00"

    def test_none_returns_none(self):
        """None input returns None."""
        assert _ms_to_iso(None) is None

    def test_recent_timestamp(self):
        """Verify conversion for a known recent date."""
        result = _ms_to_iso(1735689600000)
        assert result == "2025-01-01T00:00:00+00:00"

    def test_millisecond_precision(self):
        """Verify sub-second precision is truncated (timespec='seconds')."""
        result = _ms_to_iso(1717516800500)
        assert result == "2024-06-04T16:00:00+00:00"


class TestPolygonTickerConversion:
    """Tests for _polygon_ticker format conversion."""

    def test_stk_default(self):
        """STK tickers pass through unchanged."""
        assert _polygon_ticker({"symbol": "AAPL", "secType": "STK"}) == "AAPL"

    def test_stk_space_to_dot(self):
        """STK tickers with spaces convert to dots (BRK B -> BRK.B)."""
        assert _polygon_ticker({"symbol": "BRK B", "secType": "STK"}) == "BRK.B"

    def test_cash_conversion(self):
        """CASH tickers convert to C: prefix (EUR.USD -> C:EURUSD)."""
        assert _polygon_ticker({"symbol": "EUR.USD", "secType": "CASH"}) == "C:EURUSD"

    def test_ind_conversion(self):
        """IND tickers convert to I: prefix."""
        assert _polygon_ticker({"symbol": "SPX", "secType": "IND"}) == "I:SPX"

    def test_fut_conversion(self):
        """FUT tickers convert to F: prefix."""
        assert _polygon_ticker({"symbol": "ES", "secType": "FUT"}) == "F:ES"

    def test_missing_sec_type_defaults_stk(self):
        """Missing secType defaults to STK behavior."""
        assert _polygon_ticker({"symbol": "AAPL"}) == "AAPL"

    def test_missing_symbol(self):
        """Missing symbol returns empty string."""
        assert _polygon_ticker({"secType": "STK"}) == ""

    def test_non_dict_input(self):
        """Non-dict input is converted via str()."""
        assert _polygon_ticker("AAPL") == "AAPL"

    def test_whitespace_stripped(self):
        """Leading/trailing whitespace is stripped."""
        assert _polygon_ticker({"symbol": "  AAPL  ", "secType": "STK"}) == "AAPL"

    def test_cash_strips_dot(self):
        """CASH tickers strip all dots from symbol."""
        assert _polygon_ticker({"symbol": "GBP.USD", "secType": "CASH"}) == "C:GBPUSD"


class TestNestedHelper:
    """Tests for _nested attribute accessor."""

    def test_nested_returns_attr(self):
        """Returns attribute value when object has it."""
        obj = type("Obj", (), {"p": 42})()
        assert _nested(obj, "p") == 42

    def test_nested_none_object(self):
        """Returns None when object is None."""
        assert _nested(None, "p") is None

    def test_nested_missing_attr(self):
        """Returns None when attribute doesn't exist."""
        obj = type("Obj", (), {})()
        assert _nested(obj, "p") is None
