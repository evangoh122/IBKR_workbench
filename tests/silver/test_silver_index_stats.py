"""
tests/silver/test_silver_index_stats.py
Silver layer: derived statistics computed from staging_yf_indices.
"""
from datetime import date, timedelta

from db.database import get_connection


def _seed_constant_price_series(conn, symbol: str, n_days: int, price: float):
    """Insert n_days of constant-price bars."""
    start = date(2020, 1, 1)
    for i in range(n_days):
        ts = (start + timedelta(days=i)).isoformat()
        conn.execute(
            """
            INSERT INTO staging_yf_indices
                (symbol, ts, open, high, low, close, adj_close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (symbol, ts, price, price, price, price, price),
        )
    conn.commit()


def test_index_stats_constant_series_has_zero_returns_and_vol(tmp_db):
    """Constant price → all returns 0, vol 0, drawdown 0, sigma-bands equal mean."""
    from etl.extract_yfinance import _compute_index_stats

    with get_connection() as conn:
        _seed_constant_price_series(conn, "FLAT", n_days=300, price=100.0)
        _compute_index_stats(conn)

        row = conn.execute(
            "SELECT ret_1d, vol_252d, drawdown, max_drawdown_to_date, "
            "       mean_252d, sigma_252d, band_plus_1, band_minus_4 "
            "FROM staging_yf_index_stats "
            "WHERE symbol = 'FLAT' "
            "ORDER BY ts DESC LIMIT 1"
        ).fetchone()

    ret_1d, vol_252d, dd, max_dd, mean, sigma, band_p1, band_m4 = row
    assert abs(ret_1d) < 1e-9
    assert vol_252d is not None and abs(vol_252d) < 1e-9
    assert dd is not None and abs(dd) < 1e-9
    assert max_dd is not None and abs(max_dd) < 1e-9
    assert mean is not None and abs(mean) < 1e-9
    assert sigma is not None and abs(sigma) < 1e-9
    assert abs(band_p1) < 1e-9
    assert abs(band_m4) < 1e-9


def test_index_stats_drawdown_detects_peak_to_trough(tmp_db):
    """100 → 80 then back up → max_drawdown_to_date = -0.20."""
    from etl.extract_yfinance import _compute_index_stats

    with get_connection() as conn:
        start = date(2020, 1, 1)
        prices = [100.0] * 50 + [80.0] * 50 + [90.0] * 50
        for i, p in enumerate(prices):
            ts = (start + timedelta(days=i)).isoformat()
            conn.execute(
                """
                INSERT INTO staging_yf_indices
                    (symbol, ts, open, high, low, close, adj_close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                ("DD", ts, p, p, p, p, p),
            )
        conn.commit()
        _compute_index_stats(conn)

        max_dd = conn.execute(
            "SELECT max_drawdown_to_date FROM staging_yf_index_stats "
            "WHERE symbol = 'DD' ORDER BY ts DESC LIMIT 1"
        ).fetchone()[0]

    assert max_dd is not None
    assert abs(max_dd - (-0.20)) < 1e-6, f"expected -0.20, got {max_dd}"


def test_index_stats_short_history_returns_null_long_windows(tmp_db):
    """30 days of data → NULL in vol_252d / mean_252d / bands."""
    from etl.extract_yfinance import _compute_index_stats

    with get_connection() as conn:
        _seed_constant_price_series(conn, "SHORT", n_days=30, price=50.0)
        _compute_index_stats(conn)

        row = conn.execute(
            "SELECT vol_252d, mean_252d, sigma_252d, band_plus_2 "
            "FROM staging_yf_index_stats "
            "WHERE symbol = 'SHORT' ORDER BY ts DESC LIMIT 1"
        ).fetchone()

    vol_252d, mean_252d, sigma_252d, band_plus_2 = row
    assert vol_252d is None
    assert mean_252d is None
    assert sigma_252d is None
    assert band_plus_2 is None


def test_index_stats_rebuild_is_idempotent(tmp_db):
    """Running _compute_index_stats twice does not double rows."""
    from etl.extract_yfinance import _compute_index_stats

    with get_connection() as conn:
        _seed_constant_price_series(conn, "IDEM", n_days=100, price=10.0)
        _compute_index_stats(conn)
        n1 = conn.execute("SELECT COUNT(*) FROM staging_yf_index_stats").fetchone()[0]
        _compute_index_stats(conn)
        n2 = conn.execute("SELECT COUNT(*) FROM staging_yf_index_stats").fetchone()[0]

    assert n1 == n2 == 100
