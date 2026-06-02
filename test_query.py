"""
tests/test_query.py
Tests for DB query helper functions.
"""
import pytest
from db.database import get_connection


def _insert_stock(conn, ticker, ts, last, volume=1_000_000):
    conn.execute("""
        INSERT INTO stock_quotes (ticker, ts, last, volume, bid, ask, close, open, high, low)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (ticker, ts, last, volume, last - 0.05, last + 0.05, last - 1, last - 0.5, last + 1, last - 1))
    conn.commit()


def _insert_option(conn, ticker, expiry, strike, right, ts, bid, ask, delta, iv):
    conn.execute("""
        INSERT INTO option_quotes
            (ticker, expiry, strike, right, ts, bid, ask, last,
             volume, open_interest, implied_vol, delta, gamma, theta, vega)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (ticker, expiry, strike, right, ts, bid, ask, (bid+ask)/2,
          100, 500, iv, delta, 0.03, -0.04, 0.10))
    conn.commit()


def test_latest_stock_quotes_returns_most_recent(tmp_db):
    import importlib, query
    importlib.reload(query)

    conn = get_connection()
    _insert_stock(conn, "AAPL", "2024-01-15T10:00:00+00:00", 180.0)
    _insert_stock(conn, "AAPL", "2024-01-15T14:30:00+00:00", 182.5)   # latest
    _insert_stock(conn, "MSFT", "2024-01-15T14:30:00+00:00", 374.0)
    conn.close()

    df = query.latest_stock_quotes()
    assert len(df) == 2
    aapl = df[df.ticker == "AAPL"].iloc[0]
    assert aapl["last"] == 182.5   # most recent


def test_stock_history_filters_by_hours(tmp_db, freezer=None):
    import importlib, query
    importlib.reload(query)
    from freezegun import freeze_time

    conn = get_connection()
    _insert_stock(conn, "AAPL", "2024-01-14T10:00:00+00:00", 178.0)  # >24h ago
    _insert_stock(conn, "AAPL", "2024-01-15T12:00:00+00:00", 181.0)  # within 24h
    _insert_stock(conn, "AAPL", "2024-01-15T14:30:00+00:00", 182.5)  # within 24h
    conn.close()

    with freeze_time("2024-01-15T15:00:00+00:00"):
        importlib.reload(query)
        df = query.stock_history("AAPL", hours=24)

    assert len(df) == 2
    assert 178.0 not in df["last"].values


def test_latest_option_quotes(tmp_db):
    import importlib, query
    importlib.reload(query)

    conn = get_connection()
    _insert_option(conn, "AAPL", "20240119", 180.0, "C",
                   "2024-01-15T10:00:00+00:00", 3.0, 3.1, 0.55, 0.28)
    _insert_option(conn, "AAPL", "20240119", 180.0, "C",
                   "2024-01-15T14:30:00+00:00", 3.2, 3.3, 0.56, 0.29)  # latest
    conn.close()

    df = query.latest_option_quotes("AAPL")
    assert len(df) == 1
    assert df.iloc[0]["bid"] == pytest.approx(3.2)


def test_option_chain_summary(tmp_db):
    import importlib, query
    importlib.reload(query)

    conn = get_connection()
    for strike in [180.0, 185.0, 190.0]:
        for right in ("C", "P"):
            conn.execute(
                "INSERT INTO option_chains (ticker,expiry,strike,right) VALUES (?,?,?,?)",
                ("AAPL", "20240119", strike, right)
            )
    conn.commit()
    conn.close()

    df = query.option_chain_summary("AAPL")
    assert len(df) == 1
    row = df.iloc[0]
    assert row["strikes"] == 3
    assert row["calls"]   == 3
    assert row["puts"]    == 3


def test_etl_run_log(tmp_db):
    import importlib, query
    importlib.reload(query)

    conn = get_connection()
    conn.execute("""
        INSERT INTO etl_runs (run_type, status, message, rows_written, started_at)
        VALUES ('stocks','ok','5 rows',5,'2024-01-15T14:30:00+00:00')
    """)
    conn.commit()
    conn.close()

    df = query.etl_run_log()
    assert len(df) == 1
    assert df.iloc[0]["run_type"] == "stocks"
