"""
tests/bronze/test_bronze_yfinance_bars.py
Bronze layer: yfinance daily bars ingestion into staging_yf_bars.
"""
from unittest.mock import patch, MagicMock
import pandas as pd

from db.database import get_connection


def test_staging_tables_exist(tmp_db):
    """init_db must create the three new staging tables."""
    with get_connection() as conn:
        tables = {
            row[0] for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
    assert "staging_yf_bars" in tables
    assert "staging_yf_indices" in tables
    assert "staging_yf_index_stats" in tables


def _fake_history_df():
    """Build a pandas DataFrame in the exact shape yfinance returns from .history()."""
    idx = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])
    return pd.DataFrame(
        {
            "Open":      [100.0, 101.0, 102.0],
            "High":      [105.0, 106.0, 107.0],
            "Low":       [ 95.0,  96.0,  97.0],
            "Close":     [102.0, 103.0, 104.0],
            "Adj Close": [101.5, 102.5, 103.5],
            "Volume":    [1_000_000, 1_100_000, 1_200_000],
            "Dividends": [0.0, 0.0, 0.25],
            "Stock Splits": [0.0, 0.0, 0.0],
        },
        index=idx,
    )


def test_yf_bars_writes_rows(tmp_db):
    """run_yf_bars_etl(['NVDA']) writes 3 rows to staging_yf_bars."""
    from etl.extract_yfinance import run_yf_bars_etl

    fake_ticker = MagicMock()
    fake_ticker.history.return_value = _fake_history_df()

    with patch("etl.extract_yfinance.yf.Ticker", return_value=fake_ticker), \
         patch("etl.extract_yfinance._RATE_DELAY", 0):
        count = run_yf_bars_etl(tickers=["NVDA"])

    assert count == 3
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT ticker, ts, open, close, adj_close, volume, dividends "
            "FROM staging_yf_bars ORDER BY ts"
        ).fetchall()
    assert rows[0] == ("NVDA", "2024-01-02", 100.0, 102.0, 101.5, 1_000_000.0, 0.0)
    assert rows[2][6] == 0.25


def test_yf_bars_idempotent(tmp_db):
    """Running twice does not duplicate rows (UNIQUE(ticker, ts))."""
    from etl.extract_yfinance import run_yf_bars_etl

    fake_ticker = MagicMock()
    fake_ticker.history.return_value = _fake_history_df()

    with patch("etl.extract_yfinance.yf.Ticker", return_value=fake_ticker), \
         patch("etl.extract_yfinance._RATE_DELAY", 0):
        run_yf_bars_etl(tickers=["NVDA"])
        run_yf_bars_etl(tickers=["NVDA"])

    with get_connection() as conn:
        n = conn.execute("SELECT COUNT(*) FROM staging_yf_bars").fetchone()[0]
    assert n == 3


def test_yf_bars_empty_history_is_logged_not_raised(tmp_db):
    """If yfinance returns an empty DataFrame, the ticker is skipped silently."""
    from etl.extract_yfinance import run_yf_bars_etl

    fake_ticker = MagicMock()
    fake_ticker.history.return_value = pd.DataFrame()

    with patch("etl.extract_yfinance.yf.Ticker", return_value=fake_ticker), \
         patch("etl.extract_yfinance._RATE_DELAY", 0):
        count = run_yf_bars_etl(tickers=["DELISTED"])

    assert count == 0
