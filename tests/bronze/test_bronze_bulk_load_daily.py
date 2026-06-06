"""
tests/bronze/test_bronze_bulk_load_daily.py
Bronze layer: Polygon day_aggs_v1 flat-file loader.
"""
import gzip
from pathlib import Path

from db.database import get_connection


def _write_fake_day_aggs_gz(path: Path, rows: list):
    """Write a Polygon-style day_aggs CSV.gz with given rows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    header = "ticker,volume,open,close,high,low,window_start,transactions"
    body = "\n".join(
        f"{r['ticker']},{r['volume']},{r['open']},{r['close']},"
        f"{r['high']},{r['low']},{r['window_start']},{r['transactions']}"
        for r in rows
    )
    with gzip.open(path, "wt") as f:
        f.write(header + "\n" + body + "\n")


def test_bulk_load_daily_filters_to_tickers(tmp_path, tmp_db, monkeypatch):
    """Only rows whose ticker is in the TICKERS set land in polygon_bars."""
    from etl import bulk_load_daily

    download_dir = tmp_path / "day_aggs" / "2024"
    fake_file = download_dir / "2024-01-02.csv.gz"
    ns = 1704206400 * 1_000_000_000
    _write_fake_day_aggs_gz(
        fake_file,
        rows=[
            {"ticker": "NVDA",  "volume": 1000, "open": 100.0, "close": 105.0,
             "high": 106.0, "low": 99.0, "window_start": ns, "transactions": 50},
            {"ticker": "BOGUS", "volume": 9999, "open": 1.0, "close": 1.0,
             "high": 1.0, "low": 1.0, "window_start": ns, "transactions": 1},
            {"ticker": "AMD",   "volume": 2000, "open": 200.0, "close": 210.0,
             "high": 212.0, "low": 198.0, "window_start": ns, "transactions": 80},
        ],
    )

    monkeypatch.setattr(bulk_load_daily, "DOWNLOAD_DIR", tmp_path / "day_aggs")
    rows_written = bulk_load_daily.run(start_year=2024, end_year=2024, skip_download=True)

    assert rows_written == 2, f"expected 2 (NVDA + AMD), got {rows_written}"

    with get_connection() as conn:
        tickers = {
            r[0] for r in conn.execute(
                "SELECT DISTINCT ticker FROM polygon_bars WHERE timespan = 'day'"
            ).fetchall()
        }
        assert tickers == {"NVDA", "AMD"}

        nvda = conn.execute(
            "SELECT open, high, low, close, volume FROM polygon_bars "
            "WHERE ticker='NVDA' AND timespan='day'"
        ).fetchone()
        assert nvda == (100.0, 106.0, 99.0, 105.0, 1000.0)


def test_bulk_load_daily_idempotent(tmp_path, tmp_db, monkeypatch):
    """Loading the same file twice does not duplicate rows."""
    from etl import bulk_load_daily

    download_dir = tmp_path / "day_aggs" / "2024"
    fake_file = download_dir / "2024-01-02.csv.gz"
    ns = 1704206400 * 1_000_000_000
    _write_fake_day_aggs_gz(
        fake_file,
        rows=[
            {"ticker": "NVDA", "volume": 1000, "open": 100.0, "close": 105.0,
             "high": 106.0, "low": 99.0, "window_start": ns, "transactions": 50},
        ],
    )

    monkeypatch.setattr(bulk_load_daily, "DOWNLOAD_DIR", tmp_path / "day_aggs")
    bulk_load_daily.run(start_year=2024, end_year=2024, skip_download=True)
    bulk_load_daily.run(start_year=2024, end_year=2024, skip_download=True)

    with get_connection() as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM polygon_bars WHERE timespan='day'"
        ).fetchone()[0]
    assert n == 1
