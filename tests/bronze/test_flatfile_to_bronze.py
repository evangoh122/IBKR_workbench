"""
tests/bronze/test_flatfile_to_bronze.py
Bronze layer: Tests the Polygon FLAT-FILE → DuckDB path in
etl/bulk_load_flatfiles.py (the S3 day-aggregate loader, NOT the REST API).

These tests build tiny synthetic gzipped CSV fixtures matching the real
flat-file schema:
    ticker,volume,open,close,high,low,window_start,transactions
(window_start is NANOSECONDS since epoch), point the loader's dataset dirs at
them via monkeypatch, and verify:

- load_stocks : rows land in polygon_bars with timespan='day', ns→ISO ts
                conversion, universe filtering, and INSERT OR IGNORE dedup.
- load_options: OPRA ticker parsing (underlying / expiry / right / strike),
                universe filtering, rows land in polygon_option_bars.
- _sql_in_list: uppercases, de-dups, and strips injection characters.
- _year_globs : empty when a year dir is missing/empty (the M1 guard),
                non-empty when a .csv.gz exists.
"""
import csv
import gzip
import io
from datetime import datetime, timezone

import pytest

import etl.bulk_load_flatfiles as ff
from db.database import get_connection


# ── Fixture helpers ───────────────────────────────────────────────────────────
_HEADER = ["ticker", "volume", "open", "close", "high", "low",
           "window_start", "transactions"]

# A fixed, deterministic trading day: 2024-01-19 16:00:00 UTC.
_DT = datetime(2024, 1, 19, 16, 0, 0, tzinfo=timezone.utc)
_WINDOW_START_NS = int(_DT.timestamp()) * 1_000_000_000  # whole seconds → ns
_EXPECTED_TS = _DT.strftime("%Y-%m-%dT%H:%M:%S+00:00")    # "2024-01-19T16:00:00+00:00"


def _write_gz_csv(path, rows):
    """Write a gzipped CSV with the flat-file header + given rows (tuples)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(_HEADER)
    for row in rows:
        writer.writerow(row)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as fh:
        fh.write(buf.getvalue())


@pytest.fixture
def stocks_dir(tmp_path, monkeypatch):
    """A tmp stocks dataset dir with a 2024/ subdir; STOCKS_DIR points at it."""
    root = tmp_path / "day_aggs_stocks"
    monkeypatch.setattr(ff, "STOCKS_DIR", root)
    return root


@pytest.fixture
def options_dir(tmp_path, monkeypatch):
    """A tmp options dataset dir with a 2024/ subdir; OPTIONS_DIR points at it."""
    root = tmp_path / "day_aggs_options"
    monkeypatch.setattr(ff, "OPTIONS_DIR", root)
    return root


# ── load_stocks ───────────────────────────────────────────────────────────────
def test_load_stocks_writes_universe_rows(tmp_db, stocks_dir):
    """Universe tickers land in polygon_bars; ns→ISO ts + timespan='day'."""
    _write_gz_csv(
        stocks_dir / "2024" / "2024-01-19.csv.gz",
        [
            # MU (universe) and AMD (universe) → kept; FAKE → dropped.
            ("MU",   "1000000", "80.0", "82.0", "83.0", "79.0", _WINDOW_START_NS, "5000"),
            ("AMD",  "2000000", "140.0", "142.0", "143.0", "139.0", _WINDOW_START_NS, "8000"),
            ("FAKE", "999",     "1.0",  "1.0",  "1.0",  "1.0",  _WINDOW_START_NS, "1"),
        ],
    )

    total = ff.load_stocks(2024, 2024, ["MU", "AMD", "NVDA"])
    assert total == 2  # FAKE filtered out

    with get_connection() as conn:
        rows = conn.execute(
            "SELECT ticker, ts, timespan, open, high, low, close, volume "
            "FROM polygon_bars ORDER BY ticker"
        ).fetchall()

    assert [r[0] for r in rows] == ["AMD", "MU"]
    # Every row is a daily bar with the correct ns→ISO timestamp.
    for r in rows:
        assert r[1] == _EXPECTED_TS
        assert r[2] == "day"
    # No non-universe ticker leaked in.
    with get_connection() as conn:
        fake = conn.execute(
            "SELECT COUNT(*) FROM polygon_bars WHERE ticker = 'FAKE'"
        ).fetchone()[0]
    assert fake == 0

    # Spot-check MU's OHLCV mapping (open/high/low/close order).
    mu = [r for r in rows if r[0] == "MU"][0]
    assert mu[3] == 80.0   # open
    assert mu[4] == 83.0   # high
    assert mu[5] == 79.0   # low
    assert mu[6] == 82.0   # close
    assert mu[7] == 1000000.0  # volume


def test_load_stocks_insert_or_ignore_dedup(tmp_db, stocks_dir):
    """Re-running the same file must not duplicate rows (INSERT OR IGNORE)."""
    _write_gz_csv(
        stocks_dir / "2024" / "2024-01-19.csv.gz",
        [("MU", "1000000", "80.0", "82.0", "83.0", "79.0", _WINDOW_START_NS, "5000")],
    )

    first = ff.load_stocks(2024, 2024, ["MU"])
    second = ff.load_stocks(2024, 2024, ["MU"])

    # load_stocks returns the running total for the universe, so both see 1.
    assert first == 1
    assert second == 1

    with get_connection() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM polygon_bars WHERE ticker = 'MU'"
        ).fetchone()[0]
    assert count == 1  # deduped on (ticker, ts, timespan)


def test_load_stocks_no_files_returns_zero(tmp_db, stocks_dir):
    """No downloaded files → guarded return of 0, no error."""
    # stocks_dir exists but has no 2024/ subdir with data.
    assert ff.load_stocks(2024, 2024, ["MU"]) == 0


# ── load_options ──────────────────────────────────────────────────────────────
def test_load_options_parses_opra_and_filters(tmp_db, options_dir):
    """OPRA ticker parses to underlying/expiry/right/strike; universe filtered."""
    _write_gz_csv(
        options_dir / "2024" / "2024-01-19.csv.gz",
        [
            # MU call, strike 80.0 (universe) → kept.
            ("O:MU240119C00080000", "1500", "2.0", "2.5", "2.6", "1.9",
             _WINDOW_START_NS, "300"),
            # MU put, strike 75.0 (universe) → kept.
            ("O:MU240119P00075000", "900", "1.0", "1.2", "1.3", "0.9",
             _WINDOW_START_NS, "150"),
            # FAKE underlying → dropped by universe filter.
            ("O:FAKE240119C00080000", "10", "0.1", "0.1", "0.1", "0.1",
             _WINDOW_START_NS, "1"),
        ],
    )

    total = ff.load_options(2024, 2024, ["MU", "NVDA"])
    assert total == 2  # FAKE dropped

    with get_connection() as conn:
        call = conn.execute(
            'SELECT underlying, expiry, "right", strike, ts, timespan '
            "FROM polygon_option_bars WHERE option_ticker = 'O:MU240119C00080000'"
        ).fetchone()

    assert call[0] == "MU"
    assert call[1] == "2024-01-19"
    assert call[2] == "call"
    assert call[3] == 80.0
    assert call[4] == _EXPECTED_TS
    assert call[5] == "day"

    with get_connection() as conn:
        put_right, put_strike = conn.execute(
            'SELECT "right", strike FROM polygon_option_bars '
            "WHERE option_ticker = 'O:MU240119P00075000'"
        ).fetchone()
        fake = conn.execute(
            "SELECT COUNT(*) FROM polygon_option_bars WHERE underlying = 'FAKE'"
        ).fetchone()[0]

    assert put_right == "put"
    assert put_strike == 75.0
    assert fake == 0


def test_load_options_no_files_returns_zero(tmp_db, options_dir):
    """No downloaded option files → guarded return of 0."""
    assert ff.load_options(2024, 2024, ["MU"]) == 0


# ── _sql_in_list ──────────────────────────────────────────────────────────────
def test_sql_in_list_uppercases_single():
    assert ff._sql_in_list(["mu"]) == "'MU'"


def test_sql_in_list_dedups_case_insensitively():
    # "MU", "mu", "MU" collapse to a single 'MU'.
    assert ff._sql_in_list(["MU", "mu", "MU"]) == "'MU'"


def test_sql_in_list_preserves_dot():
    assert ff._sql_in_list(["EUR.USD"]) == "'EUR.USD'"


def test_sql_in_list_strips_injection_chars():
    # Quotes, semicolons, spaces and dashes are stripped; only [A-Z0-9.] survive.
    result = ff._sql_in_list(["MU'; DROP TABLE x;--"])
    assert result == "'MUDROPTABLEX'"
    assert ";" not in result
    assert "--" not in result
    # The only single quotes present are the wrapping literal delimiters.
    assert result.count("'") == 2


def test_sql_in_list_drops_empty_after_cleaning():
    # A symbol that cleans to empty is dropped entirely.
    assert ff._sql_in_list(["!!!", "MU"]) == "'MU'"


# ── _year_globs (the M1 empty/missing-year guard) ─────────────────────────────
def test_year_globs_missing_year_dir(tmp_path):
    """A year with no directory yields no glob (avoids DuckDB's no-files abort)."""
    assert ff._year_globs(tmp_path, 2025, 2025) == []


def test_year_globs_empty_year_dir(tmp_path):
    """A year dir that exists but holds no .csv.gz yields no glob."""
    (tmp_path / "2025").mkdir()
    assert ff._year_globs(tmp_path, 2025, 2025) == []


def test_year_globs_nonempty_year_dir(tmp_path):
    """A year dir with a .csv.gz yields exactly one glob referencing that year."""
    _write_gz_csv(
        tmp_path / "2024" / "2024-01-19.csv.gz",
        [("MU", "1", "1.0", "1.0", "1.0", "1.0", _WINDOW_START_NS, "1")],
    )
    globs = ff._year_globs(tmp_path, 2024, 2024)
    assert len(globs) == 1
    assert "2024" in globs[0]
    assert globs[0].endswith("/**/*.csv.gz")
    assert "\\" not in globs[0]  # forward slashes for DuckDB
