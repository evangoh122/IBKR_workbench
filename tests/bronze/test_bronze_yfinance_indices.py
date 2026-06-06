"""
tests/bronze/test_bronze_yfinance_indices.py
Bronze layer: yfinance major-indices ingestion + derived spread symbols.
"""
from unittest.mock import patch, MagicMock
import pandas as pd

from db.database import get_connection


def _two_day_df(close_left: float, close_right: float):
    """Tiny 2-row df with matching dates for spread arithmetic."""
    idx = pd.to_datetime(["2024-01-02", "2024-01-03"])
    return pd.DataFrame(
        {
            "Open":      [100.0, 101.0],
            "High":      [105.0, 106.0],
            "Low":       [ 95.0,  96.0],
            "Close":     [close_left, close_left + 1.0],
            "Adj Close": [close_left, close_left + 1.0],
            "Volume":    [1_000_000, 1_100_000],
        },
        index=idx,
    )


def test_yf_indices_writes_each_symbol(tmp_db):
    """run_yf_indices_etl iterates INDICES and inserts rows per symbol."""
    from etl.extract_yfinance import run_yf_indices_etl, INDICES

    def fake_ticker_factory(sym):
        m = MagicMock()
        m.history.return_value = _two_day_df(close_left=200.0, close_right=0.0)
        return m

    with patch("etl.extract_yfinance.yf.Ticker", side_effect=fake_ticker_factory), \
         patch("etl.extract_yfinance._RATE_DELAY", 0), \
         patch("etl.extract_yfinance._compute_index_stats", return_value=0):
        run_yf_indices_etl()

    with get_connection() as conn:
        symbols = {
            r[0] for r in conn.execute(
                "SELECT DISTINCT symbol FROM staging_yf_indices"
            ).fetchall()
        }
    for sym in INDICES:
        assert sym in symbols, f"{sym} missing from staging_yf_indices"


def test_yf_indices_derived_spreads_are_correct(tmp_db):
    """Derived rows ^IXIC_MINUS_GSPC and ^RUT_MINUS_GSPC equal close_left - close_right."""
    from etl.extract_yfinance import run_yf_indices_etl

    closes_per_symbol = {
        "^IXIC": 16000.0,
        "^GSPC": 4800.0,
        "^RUT":  2100.0,
    }

    def fake_ticker_factory(sym):
        m = MagicMock()
        base = closes_per_symbol.get(sym, 100.0)
        idx = pd.to_datetime(["2024-01-02", "2024-01-03"])
        m.history.return_value = pd.DataFrame(
            {
                "Open":      [base, base + 1.0],
                "High":      [base + 5, base + 6],
                "Low":       [base - 5, base - 4],
                "Close":     [base, base + 1.0],
                "Adj Close": [base, base + 1.0],
                "Volume":    [1, 1],
            },
            index=idx,
        )
        return m

    with patch("etl.extract_yfinance.yf.Ticker", side_effect=fake_ticker_factory), \
         patch("etl.extract_yfinance._RATE_DELAY", 0), \
         patch("etl.extract_yfinance._compute_index_stats", return_value=0):
        run_yf_indices_etl()

    with get_connection() as conn:
        rows = conn.execute(
            "SELECT ts, close FROM staging_yf_indices "
            "WHERE symbol = '^IXIC_MINUS_GSPC' ORDER BY ts"
        ).fetchall()
        assert rows[0] == ("2024-01-02", 16000.0 - 4800.0)
        assert rows[1] == ("2024-01-03", 16001.0 - 4801.0)

        rut_rows = conn.execute(
            "SELECT close FROM staging_yf_indices "
            "WHERE symbol = '^RUT_MINUS_GSPC' ORDER BY ts"
        ).fetchall()
        assert rut_rows[0][0] == 2100.0 - 4800.0
        assert rut_rows[1][0] == 2101.0 - 4801.0
