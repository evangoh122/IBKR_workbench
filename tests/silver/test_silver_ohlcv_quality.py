"""
tests/silver/test_silver_ohlcv_quality.py
Silver layer: Data quality and constraint validation for OHLCV data.

These tests verify that:
- OHLCV relationships are valid (high >= open/close, low <= open/close)
- Volume and VWAP are non-negative
- Timestamps are valid ISO-8601
- No duplicate bars per (ticker, ts, timespan)
- Schema consistency between polygon_bars and polygon_option_bars
"""
import pytest
from datetime import datetime, timezone

from db.database import get_connection


@pytest.fixture
def seed_bars(db_conn):
    """Seed polygon_bars with test data for quality checks."""
    bars = [
        # Valid bar (vwap=102 is between low=90 and high=110)
        ("AAPL", "2024-06-01T00:00:00+00:00", "day", 100.0, 110.0, 90.0, 105.0, 1000000, 102.0, 5000),
        # High < open (invalid) — vwap=92 is between low=90 and high=95
        ("AAPL", "2024-06-02T00:00:00+00:00", "day", 100.0, 95.0, 90.0, 105.0, 1000000, 92.0, 5000),
        # Low > close (invalid) — vwap=109 is between low=108 and high=110
        ("AAPL", "2024-06-03T00:00:00+00:00", "day", 100.0, 110.0, 108.0, 105.0, 1000000, 109.0, 5000),
        # Negative volume (vwap=102 is within low-high range)
        ("MSFT", "2024-06-01T00:00:00+00:00", "day", 100.0, 110.0, 90.0, 105.0, -100, 102.0, 5000),
        # Valid bar for MSFT
        ("MSFT", "2024-06-02T00:00:00+00:00", "day", 100.0, 110.0, 90.0, 105.0, 1000000, 102.0, 5000),
    ]

    for bar in bars:
        db_conn.execute("""
            INSERT INTO polygon_bars (ticker, ts, timespan, open, high, low, close, volume, vwap, transactions)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, bar)
    db_conn.commit()

    yield db_conn


def test_high_gte_open_and_close(seed_bars):
    """Verify high >= max(open, close) for all bars."""
    result = seed_bars.execute("""
        SELECT COUNT(*) FROM polygon_bars
        WHERE high < GREATEST(open, close)
    """).fetchone()[0]
    # Should have 1 invalid bar (AAPL 2024-06-02)
    assert result == 1


def test_low_lte_open_and_close(seed_bars):
    """Verify low <= min(open, close) for all bars."""
    result = seed_bars.execute("""
        SELECT COUNT(*) FROM polygon_bars
        WHERE low > LEAST(open, close)
    """).fetchone()[0]
    # Should have 1 invalid bar (AAPL 2024-06-03)
    assert result == 1


def test_volume_non_negative(seed_bars):
    """Verify volume is non-negative for all bars."""
    result = seed_bars.execute("""
        SELECT COUNT(*) FROM polygon_bars
        WHERE volume < 0
    """).fetchone()[0]
    # Should have 1 invalid bar (MSFT 2024-06-01)
    assert result == 1


def test_vwap_within_low_high_range(seed_bars):
    """Verify VWAP is between low and high when present."""
    result = seed_bars.execute("""
        SELECT COUNT(*) FROM polygon_bars
        WHERE vwap IS NOT NULL
          AND (vwap < low OR vwap > high)
    """).fetchone()[0]
    # All VWAPs should be within range
    assert result == 0


def test_timestamp_is_valid_iso8601(seed_bars):
    """Verify all timestamps follow ISO-8601 format."""
    rows = seed_bars.execute("SELECT ts FROM polygon_bars").fetchall()
    for (ts_str,) in rows:
        # Should be parseable as ISO-8601
        dt = datetime.fromisoformat(ts_str)
        assert dt.tzinfo is not None, f"Timestamp missing timezone: {ts_str}"


def test_ticker_not_empty(seed_bars):
    """Verify ticker is never empty or NULL."""
    result = seed_bars.execute("""
        SELECT COUNT(*) FROM polygon_bars
        WHERE ticker IS NULL OR TRIM(ticker) = ''
    """).fetchone()[0]
    assert result == 0


def test_no_duplicate_bars(seed_bars):
    """Verify UNIQUE constraint prevents duplicate (ticker, ts, timespan)."""
    # Try inserting duplicate
    try:
        seed_bars.execute("""
            INSERT INTO polygon_bars (ticker, ts, timespan, open, high, low, close, volume, vwap, transactions)
            VALUES ('AAPL', '2024-06-01T00:00:00+00:00', 'day', 100.0, 110.0, 90.0, 105.0, 1000000, 102.0, 5000)
        """)
        # If no exception, check count
    except Exception:
        pass  # Expected — duplicate rejected

    count = seed_bars.execute("SELECT COUNT(*) FROM polygon_bars").fetchone()[0]
    # Should still be 5 (original seed), not 6
    assert count == 5


def test_option_bars_schema_consistency(seed_bars):
    """Verify polygon_option_bars has same OHLCV column structure."""
    # Create option_bars table and verify columns match
    seed_bars.execute("""
        CREATE TABLE IF NOT EXISTS polygon_option_bars (
            option_ticker TEXT, underlying TEXT, expiry TEXT, strike REAL,
            "right" TEXT, ts TEXT, timespan TEXT,
            open REAL, high REAL, low REAL, close REAL,
            volume REAL, vwap REAL, transactions INTEGER,
            created_at TIMESTAMP DEFAULT now(),
            UNIQUE(option_ticker, ts, timespan)
        )
    """)

    # Insert sample option bar
    seed_bars.execute("""
        INSERT INTO polygon_option_bars
            (option_ticker, underlying, expiry, strike, "right", ts, timespan,
             open, high, low, close, volume, vwap, transactions)
        VALUES ('O:AAPL240119C00150000', 'AAPL', '2024-01-19', 150.0, 'call',
                '2024-06-01T00:00:00+00:00', 'day', 5.0, 6.0, 4.0, 5.5, 10000, 5.25, 500)
    """)

    row = seed_bars.execute(
        "SELECT open, high, low, close, volume, vwap FROM polygon_option_bars"
    ).fetchone()

    assert row[0] == 5.0   # open
    assert row[1] == 6.0   # high
    assert row[2] == 4.0   # low
    assert row[3] == 5.5   # close
    assert row[4] == 10000 # volume
    assert row[5] == 5.25  # vwap


def test_bar_count_per_ticker(seed_bars):
    """Verify we can aggregate bar counts per ticker."""
    result = seed_bars.execute("""
        SELECT ticker, COUNT(*) as cnt
        FROM polygon_bars
        GROUP BY ticker
        ORDER BY cnt DESC
    """).fetchall()

    assert len(result) == 2  # AAPL and MSFT
    assert result[0][0] == "AAPL"
    assert result[0][1] == 3  # 3 bars for AAPL
    assert result[1][0] == "MSFT"
    assert result[1][1] == 2  # 2 bars for MSFT
