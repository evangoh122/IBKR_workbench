"""
tests/test_ohlc_pipeline.py
Unit tests for OHLC data pipeline across bronze/silver/gold medallion layers.

Tests the flow of data:
  Bronze: Raw trades (polygon_trades) → Silver: Aggregated bars (polygon_bars)
  Silver: OHLC bars → Gold: Derived metrics (latest bar, aggregates)

Also tests group filtering for semiconductors and 5-year lookback behavior.
"""
import pytest
from datetime import datetime, timedelta, timezone
import duckdb


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_trades():
    """Generate 500 raw trade ticks for AAPL across a single day."""
    trades = []
    base_ts = datetime(2024, 6, 1, 9, 30, 0, tzinfo=timezone.utc)
    for i in range(500):
        ts = (base_ts + timedelta(seconds=i * 12)).isoformat(timespec="seconds")
        price = 190.0 + (i % 20) * 0.25  # oscillate between 190-195
        trades.append({
            "ticker": "AAPL",
            "ts": ts,
            "price": price,
            "size": 100 + i * 10,
            "conditions": "12,16" if i % 5 == 0 else None,
            "exchange": 4,
            "tape": "A",
        })
    return trades


@pytest.fixture
def sample_multi_ticker_trades():
    """Generate trades across 3 tickers (NVDA, AMD, INTC) for cross-ticker tests."""
    trades = []
    tickers = ["NVDA", "AMD", "INTC"]
    base_ts = datetime(2024, 6, 1, 9, 30, 0, tzinfo=timezone.utc)
    for ticker in tickers:
        for i in range(100):
            ts = (base_ts + timedelta(seconds=i * 60)).isoformat(timespec="seconds")
            price = {"NVDA": 800.0, "AMD": 150.0, "INTC": 30.0}[ticker] + (i % 10) * 0.5
            trades.append({
                "ticker": ticker,
                "ts": ts,
                "price": price,
                "size": 50 + i * 5,
                "conditions": None,
                "exchange": 4,
                "tape": "A",
            })
    return trades


@pytest.fixture
def ohlc_bars_5years():
    """Generate 1825 daily bars (5 years) for a single ticker."""
    bars = []
    base_ts = datetime(2021, 1, 4, tzinfo=timezone.utc)  # first trading day
    for i in range(1825):
        ts = (base_ts + timedelta(days=i)).isoformat(timespec="seconds")
        price = 130.0 + i * 0.03  # slow uptrend
        bars.append({
            "ticker": "NVDA",
            "ts": ts,
            "timespan": "day",
            "open": price,
            "high": price + 3.0,
            "low": price - 2.0,
            "close": price + 0.5,
            "volume": 50_000_000 + i * 100_000,
            "vwap": price + 0.25,
            "transactions": 200_000 + i * 500,
        })
    return bars


# ── Bronze Layer Tests ────────────────────────────────────────────────────────

class TestBronzeTradesIngestion:
    """Verify raw trades land correctly in polygon_trades."""

    def test_insert_500_trades(self, tmp_db, sample_trades):
        """Verify 500 raw trades insert into polygon_trades."""
        with duckdb.connect(tmp_db) as conn:
            for t in sample_trades:
                conditions = t["conditions"]
                conn.execute("""
                    INSERT OR IGNORE INTO polygon_trades
                        (ticker, ts, price, size, conditions, exchange, tape)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (t["ticker"], t["ts"], t["price"], t["size"],
                      conditions, t["exchange"], t["tape"]))

            count = conn.execute("SELECT COUNT(*) FROM polygon_trades").fetchone()[0]
            assert count == 500

    def test_dedup_on_ticker_ts_exchange(self, tmp_db, sample_trades):
        """Verify UNIQUE(ticker, ts, exchange) prevents duplicates."""
        with duckdb.connect(tmp_db) as conn:
            for _ in range(2):  # insert twice
                for t in sample_trades:
                    conn.execute("""
                        INSERT OR IGNORE INTO polygon_trades
                            (ticker, ts, price, size, conditions, exchange, tape)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (t["ticker"], t["ts"], t["price"], t["size"],
                          t["conditions"], t["exchange"], t["tape"]))

            count = conn.execute("SELECT COUNT(*) FROM polygon_trades").fetchone()[0]
            assert count == 500, "Duplicates should be ignored"

    def test_price_within_ohlc_range(self, tmp_db, sample_trades):
        """Verify raw trade prices are usable for OHLC aggregation."""
        with duckdb.connect(tmp_db) as conn:
            for t in sample_trades:
                conn.execute("""
                    INSERT OR IGNORE INTO polygon_trades
                        (ticker, ts, price, size, conditions, exchange, tape)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (t["ticker"], t["ts"], t["price"], t["size"],
                      t["conditions"], t["exchange"], t["tape"]))

            # Aggregate to daily bar
            conn.execute("""
                INSERT OR IGNORE INTO polygon_bars
                    (ticker, ts, timespan, open, high, low, close, volume, transactions)
                SELECT
                    ticker,
                    DATE_TRUNC('day', ts::TIMESTAMP) as ts,
                    'day' as timespan,
                    MIN(price) as open,
                    MAX(price) as high,
                    MIN(price) as low,
                    MAX(price) as close,
                    SUM(size) as volume,
                    COUNT(*) as transactions
                FROM polygon_trades
                GROUP BY ticker, DATE_TRUNC('day', ts::TIMESTAMP)
            """)

            bar = conn.execute(
                "SELECT open, high, low, close FROM polygon_bars"
            ).fetchone()
            assert bar[1] >= bar[0], "High >= open"
            assert bar[2] <= bar[1], "Low <= high"

    def test_multi_ticker_bronze(self, tmp_db, sample_multi_ticker_trades):
        """Verify trades for multiple tickers are stored correctly."""
        with duckdb.connect(tmp_db) as conn:
            for t in sample_multi_ticker_trades:
                conn.execute("""
                    INSERT OR IGNORE INTO polygon_trades
                        (ticker, ts, price, size, conditions, exchange, tape)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (t["ticker"], t["ts"], t["price"], t["size"],
                      t["conditions"], t["exchange"], t["tape"]))

            result = conn.execute("""
                SELECT ticker, COUNT(*) as cnt
                FROM polygon_trades
                GROUP BY ticker
                ORDER BY ticker
            """).fetchall()

            assert len(result) == 3
            assert result[0][0] == "AMD"  # 100
            assert result[1][0] == "INTC"  # 100
            assert result[2][0] == "NVDA"  # 100


# ── Silver Layer Tests ────────────────────────────────────────────────────────

class TestSilverBarsAggregation:
    """Verify trades aggregate correctly into OHLC bars."""

    def test_bronze_to_silver_daily_bar(self, tmp_db, sample_trades):
        """Verify 500 trades aggregate into 1 daily bar."""
        with duckdb.connect(tmp_db) as conn:
            for t in sample_trades:
                conn.execute("""
                    INSERT OR IGNORE INTO polygon_trades
                        (ticker, ts, price, size, conditions, exchange, tape)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (t["ticker"], t["ts"], t["price"], t["size"],
                      t["conditions"], t["exchange"], t["tape"]))

            # Aggregate into daily bar
            conn.execute("""
                INSERT OR IGNORE INTO polygon_bars
                    (ticker, ts, timespan, open, high, low, close, volume, transactions)
                SELECT
                    ticker,
                    DATE_TRUNC('day', ts::TIMESTAMP) as ts,
                    'day' as timespan,
                    MIN(price) as open,
                    MAX(price) as high,
                    MIN(price) as low,
                    MAX(price) as close,
                    SUM(size) as volume,
                    COUNT(*) as transactions
                FROM polygon_trades
                GROUP BY ticker, DATE_TRUNC('day', ts::TIMESTAMP)
            """)

            count = conn.execute("SELECT COUNT(*) FROM polygon_bars").fetchone()[0]
            assert count == 1, "500 trades on 1 day = 1 daily bar"

            bar = conn.execute(
                "SELECT open, high, low, close, volume, transactions FROM polygon_bars"
            ).fetchone()
            assert bar[5] == 500, "transactions = 500"
            assert bar[4] > 0, "volume > 0"

    def test_silver_dedup_idempotent(self, tmp_db, sample_trades):
        """Verify running aggregation twice doesn't create duplicates."""
        with duckdb.connect(tmp_db) as conn:
            for t in sample_trades:
                conn.execute("""
                    INSERT OR IGNORE INTO polygon_trades
                        (ticker, ts, price, size, conditions, exchange, tape)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (t["ticker"], t["ts"], t["price"], t["size"],
                      t["conditions"], t["exchange"], t["tape"]))

            # Aggregate twice
            for _ in range(2):
                conn.execute("""
                    INSERT OR IGNORE INTO polygon_bars
                        (ticker, ts, timespan, open, high, low, close, volume, transactions)
                    SELECT
                        ticker,
                        DATE_TRUNC('day', ts::TIMESTAMP) as ts,
                        'day' as timespan,
                        MIN(price) as open,
                        MAX(price) as high,
                        MIN(price) as low,
                        MAX(price) as close,
                        SUM(size) as volume,
                        COUNT(*) as transactions
                    FROM polygon_trades
                    GROUP BY ticker, DATE_TRUNC('day', ts::TIMESTAMP)
                """)

            count = conn.execute("SELECT COUNT(*) FROM polygon_bars").fetchone()[0]
            assert count == 1, "Idempotent insert should not create duplicates"

    def test_multi_ticker_silver(self, tmp_db, sample_multi_ticker_trades):
        """Verify trades aggregate into separate bars per ticker."""
        with duckdb.connect(tmp_db) as conn:
            for t in sample_multi_ticker_trades:
                conn.execute("""
                    INSERT OR IGNORE INTO polygon_trades
                        (ticker, ts, price, size, conditions, exchange, tape)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (t["ticker"], t["ts"], t["price"], t["size"],
                      t["conditions"], t["exchange"], t["tape"]))

            conn.execute("""
                INSERT OR IGNORE INTO polygon_bars
                    (ticker, ts, timespan, open, high, low, close, volume, transactions)
                SELECT
                    ticker,
                    DATE_TRUNC('day', ts::TIMESTAMP) as ts,
                    'day' as timespan,
                    MIN(price) as open,
                    MAX(price) as high,
                    MIN(price) as low,
                    MAX(price) as close,
                    SUM(size) as volume,
                    COUNT(*) as transactions
                FROM polygon_trades
                GROUP BY ticker, DATE_TRUNC('day', ts::TIMESTAMP)
            """)

            result = conn.execute("""
                SELECT ticker, COUNT(*) as cnt
                FROM polygon_bars
                GROUP BY ticker
                ORDER BY ticker
            """).fetchall()

            assert len(result) == 3
            for row in result:
                assert row[1] == 1, f"{row[0]} should have 1 daily bar"


# ── Gold Layer Tests ──────────────────────────────────────────────────────────

class TestGoldDerivedMetrics:
    """Verify gold-layer queries derived from silver bars."""

    def test_latest_bar_per_ticker(self, tmp_db, db_conn):
        """Verify window function returns most recent bar per ticker."""
        base_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        for i in range(5):
            ts = (base_ts + timedelta(days=i)).isoformat(timespec="seconds")
            db_conn.execute("""
                INSERT OR IGNORE INTO polygon_bars
                    (ticker, ts, timespan, open, high, low, close, volume, transactions)
                VALUES (?, ?, 'day', ?, ?, ?, ?, ?, ?)
            """, ("NVDA", ts, 800.0 + i, 810.0 + i, 790.0 + i, 805.0 + i, 50_000_000, 100_000))

        result = db_conn.execute("""
            WITH ranked AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ts DESC) as rn
                FROM polygon_bars
            )
            SELECT ticker, ts, close FROM ranked WHERE rn = 1
        """).fetchone()

        assert result[0] == "NVDA"
        assert result[2] == 809.0  # last close (805.0 + 4)

    def test_5year_lookback_aggregate(self, tmp_db, db_conn, ohlc_bars_5years):
        """Verify 5-year dataset aggregates correctly (1825 daily bars)."""
        for bar in ohlc_bars_5years:
            db_conn.execute("""
                INSERT OR IGNORE INTO polygon_bars
                    (ticker, ts, timespan, open, high, low, close, volume, vwap, transactions)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (bar["ticker"], bar["ts"], bar["timespan"],
                  bar["open"], bar["high"], bar["low"], bar["close"],
                  bar["volume"], bar["vwap"], bar["transactions"]))

        count = db_conn.execute("SELECT COUNT(*) FROM polygon_bars").fetchone()[0]
        assert count == 1825, f"Expected 1825 bars, got {count}"

        # Verify aggregate metrics
        result = db_conn.execute("""
            SELECT
                MIN(open) as period_low,
                MAX(high) as period_high,
                AVG(close) as avg_close,
                SUM(volume) as total_volume
            FROM polygon_bars
        """).fetchone()

        assert result[0] < result[1], "period_low < period_high"
        assert result[2] > 0, "avg_close > 0"
        assert result[3] > 0, "total_volume > 0"

    def test_gold_derives_from_silver(self, tmp_db, db_conn):
        """Verify gold-layer summary can be derived from silver bars."""
        # Silver bars
        for i in range(10):
            ts = (datetime(2024, 6, 1, tzinfo=timezone.utc) + timedelta(days=i)).isoformat(timespec="seconds")
            db_conn.execute("""
                INSERT OR IGNORE INTO polygon_bars
                    (ticker, ts, timespan, open, high, low, close, volume, transactions)
                VALUES (?, ?, 'day', ?, ?, ?, ?, ?, ?)
            """, ("AAPL", ts, 190.0 + i, 195.0 + i, 188.0 + i, 192.0 + i, 70_000_000, 200_000))

        # Gold: derive 10-day summary
        result = db_conn.execute("""
            SELECT
                ticker,
                AVG(close) as avg_10d,
                SUM(volume) as vol_10d,
                MAX(high) as high_10d,
                MIN(low) as low_10d,
                (MAX(high) - MIN(low)) as range_10d
            FROM polygon_bars
            GROUP BY ticker
        """).fetchone()

        assert result[0] == "AAPL"
        assert result[1] > 0
        assert result[4] <= result[3], "low_10d <= high_10d"
        assert result[5] > 0, "range > 0"


# ── Pipeline Flow Tests ───────────────────────────────────────────────────────

class TestPipelineFlow:
    """End-to-end data movement through medallion layers."""

    def test_full_bronze_silver_gold_pipeline(self, tmp_db, db_conn, sample_multi_ticker_trades):
        """End-to-end: raw trades → daily bars → gold summary."""
        # Bronze: insert trades
        for t in sample_multi_ticker_trades:
            db_conn.execute("""
                INSERT OR IGNORE INTO polygon_trades
                    (ticker, ts, price, size, conditions, exchange, tape)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (t["ticker"], t["ts"], t["price"], t["size"],
                  t["conditions"], t["exchange"], t["tape"]))

        bronze = db_conn.execute("SELECT COUNT(*) FROM polygon_trades").fetchone()[0]
        assert bronze == 300, "300 raw trades"

        # Silver: aggregate to daily bars
        db_conn.execute("""
            INSERT OR IGNORE INTO polygon_bars
                (ticker, ts, timespan, open, high, low, close, volume, transactions)
            SELECT
                ticker,
                DATE_TRUNC('day', ts::TIMESTAMP) as ts,
                'day' as timespan,
                MIN(price) as open,
                MAX(price) as high,
                MIN(price) as low,
                MAX(price) as close,
                SUM(size) as volume,
                COUNT(*) as transactions
            FROM polygon_trades
            GROUP BY ticker, DATE_TRUNC('day', ts::TIMESTAMP)
        """)

        silver = db_conn.execute("SELECT COUNT(*) FROM polygon_bars").fetchone()[0]
        assert silver == 3, "3 tickers × 1 day = 3 bars"

        # Gold: derive per-ticker summary
        result = db_conn.execute("""
            SELECT
                ticker,
                ROUND(AVG(close), 2) as avg_close,
                SUM(volume) as total_volume,
                MAX(high) - MIN(low) as price_range
            FROM polygon_bars
            GROUP BY ticker
            ORDER BY ticker
        """).fetchall()

        assert len(result) == 3
        for row in result:
            assert row[1] > 0, f"{row[0]} avg_close > 0"
            assert row[2] > 0, f"{row[0]} total_volume > 0"
            assert row[3] > 0, f"{row[0]} price_range > 0"

    def test_large_volume_pipeline(self, tmp_db, db_conn):
        """Verify pipeline handles 10k rows without issues."""
        tickers = ["NVDA", "AMD", "INTC", "QCOM", "AVGO"]
        base_ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
        for ticker in tickers:
            for i in range(2000):
                ts = (base_ts + timedelta(days=i)).isoformat(timespec="seconds")
                price = {"NVDA": 800, "AMD": 150, "INTC": 30, "QCOM": 120, "AVGO": 600}[ticker] + i * 0.01
                db_conn.execute("""
                    INSERT OR IGNORE INTO polygon_bars
                        (ticker, ts, timespan, open, high, low, close, volume, vwap, transactions)
                    VALUES (?, ?, 'day', ?, ?, ?, ?, ?, ?, ?)
                """, (ticker, ts, price, price + 5, price - 5, price + 0.5, 50_000_000, price, 200_000))

        total = db_conn.execute("SELECT COUNT(*) FROM polygon_bars").fetchone()[0]
        assert total == 10000

        # Verify aggregation works on large dataset
        result = db_conn.execute("""
            SELECT ticker, COUNT(*), AVG(close)
            FROM polygon_bars
            GROUP BY ticker
        """).fetchall()
        assert len(result) == 5
        for row in result:
            assert row[1] == 2000

    def test_cross_ticker_filter(self, tmp_db, db_conn, sample_multi_ticker_trades):
        """Verify queries can filter by specific ticker."""
        for t in sample_multi_ticker_trades:
            db_conn.execute("""
                INSERT OR IGNORE INTO polygon_trades
                    (ticker, ts, price, size, conditions, exchange, tape)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (t["ticker"], t["ts"], t["price"], t["size"],
                  t["conditions"], t["exchange"], t["tape"]))

        db_conn.execute("""
            INSERT OR IGNORE INTO polygon_bars
                (ticker, ts, timespan, open, high, low, close, volume, transactions)
            SELECT
                ticker,
                DATE_TRUNC('day', ts::TIMESTAMP) as ts,
                'day' as timespan,
                MIN(price) as open,
                MAX(price) as high,
                MIN(price) as low,
                MAX(price) as close,
                SUM(size) as volume,
                COUNT(*) as transactions
            FROM polygon_trades
            GROUP BY ticker, DATE_TRUNC('day', ts::TIMESTAMP)
        """)

        # Filter for NVDA only
        result = db_conn.execute("""
            SELECT ticker, close, volume FROM polygon_bars WHERE ticker = 'NVDA'
        """).fetchall()

        assert len(result) == 1
        assert result[0][0] == "NVDA"


# ── Group Filtering Tests ─────────────────────────────────────────────────────

class TestGroupFiltering:
    """Verify POLYGON_GROUPS filtering works for semiconductors."""

    def test_get_tickers_by_groups(self):
        """Verify get_tickers_by_groups returns correct tickers."""
        from config.tickers import get_tickers_by_groups

        semis = get_tickers_by_groups(["semiconductors"])
        assert len(semis) > 0
        symbols = [t["symbol"] for t in semis]
        assert "NVDA" in symbols
        assert "AMD" in symbols
        assert "INTC" in symbols

    def test_combined_semis_groups(self):
        """Verify combining both semiconductor groups."""
        from config.tickers import get_tickers_by_groups

        all_semis = get_tickers_by_groups([
            "semiconductors",
            "semiconductor_equipment_and_materials"
        ])
        symbols = [t["symbol"] for t in all_semis]
        # Should have both chip designers and equipment makers
        assert "NVDA" in symbols  # semiconductors
        assert "ASML" in symbols  # semiconductor_equipment_and_materials
        assert "AMAT" in symbols  # semiconductor_equipment_and_materials
        # No duplicates
        assert len(all_semis) == len(set(symbols))
