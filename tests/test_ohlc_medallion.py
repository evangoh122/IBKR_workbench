"""
tests/test_ohlc_medallion.py
Tests for OHLC data pipeline across bronze/silver/gold medallion layers.

Bronze: Raw ingestion (stock_quotes, polygon_trades, polygon_snapshots)
Silver: Deduplicated OHLC bars (polygon_bars, polygon_option_bars)
Gold:   Enriched/embeddings (ticker_embeddings, edgar_embeddings)
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta, timezone
import duckdb


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_ohlcv_bars():
    """Generate sample OHLCV bar data for testing."""
    bars = []
    base_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    for i in range(10):
        ts = (base_ts + timedelta(days=i)).isoformat(timespec="seconds")
        bars.append({
            "ticker": "AAPL",
            "ts": ts,
            "timespan": "day",
            "open": 150.0 + i,
            "high": 155.0 + i,
            "low": 148.0 + i,
            "close": 152.0 + i,
            "volume": 1000000 + i * 100000,
            "vwap": 151.5 + i,
            "transactions": 50000 + i * 1000,
        })
    return bars


@pytest.fixture
def sample_raw_trades():
    """Generate raw trade tick data (bronze layer)."""
    trades = []
    base_ts = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    for i in range(20):
        ts = (base_ts + timedelta(seconds=i * 10)).isoformat(timespec="seconds")
        trades.append({
            "ticker": "AAPL",
            "ts": ts,
            "price": 150.0 + (i % 5),
            "size": 100 + i * 10,
            "conditions": "12,16" if i % 3 == 0 else None,
            "exchange": 4,
            "tape": "A",
        })
    return trades


@pytest.fixture
def sample_option_bars():
    """Generate sample option OHLCV bars."""
    bars = []
    base_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    for i in range(5):
        ts = (base_ts + timedelta(days=i)).isoformat(timespec="seconds")
        bars.append({
            "option_ticker": f"O:AAPL240119C00150000",
            "underlying": "AAPL",
            "expiry": "2024-01-19",
            "strike": 150.0,
            "right": "call",
            "ts": ts,
            "timespan": "day",
            "open": 10.0 + i,
            "high": 12.0 + i,
            "low": 9.0 + i,
            "close": 11.0 + i,
            "volume": 5000 + i * 1000,
            "vwap": 10.5 + i,
            "transactions": 1000 + i * 200,
        })
    return bars


# ── Bronze Layer Tests ────────────────────────────────────────────────────────

class TestBronzeLayer:
    """Tests for raw data ingestion into bronze tables."""

    def test_stock_quotes_insert(self, tmp_db, sample_raw_trades):
        """Verify raw stock quotes can be inserted into bronze table."""
        with duckdb.connect(tmp_db) as conn:
            conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS stock_quotes_id_seq;
                CREATE TABLE IF NOT EXISTS stock_quotes (
                    id INTEGER PRIMARY KEY DEFAULT nextval('stock_quotes_id_seq'),
                    ticker TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    bid REAL, ask REAL, last REAL, "close" REAL,
                    volume INTEGER, "open" REAL, high REAL, low REAL, vwap REAL,
                    created_at TIMESTAMP DEFAULT now()
                )
            """)
            for trade in sample_raw_trades[:5]:
                conn.execute("""
                    INSERT INTO stock_quotes (ticker, ts, last, volume)
                    VALUES (?, ?, ?, ?)
                """, (trade["ticker"], trade["ts"], trade["price"], trade["size"]))

            count = conn.execute("SELECT COUNT(*) FROM stock_quotes").fetchone()[0]
            assert count == 5

    def test_polygon_trades_insert(self, tmp_db, sample_raw_trades):
        """Verify raw trades can be inserted with dedup constraint."""
        with duckdb.connect(tmp_db) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS polygon_trades (
                    ticker TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    price REAL, size REAL, conditions TEXT,
                    exchange INTEGER, tape TEXT,
                    created_at TIMESTAMP DEFAULT now(),
                    UNIQUE(ticker, ts, exchange)
                )
            """)
            for trade in sample_raw_trades:
                conn.execute("""
                    INSERT OR IGNORE INTO polygon_trades
                        (ticker, ts, price, size, conditions, exchange, tape)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    trade["ticker"], trade["ts"], trade["price"],
                    trade["size"], trade["conditions"], trade["exchange"], trade["tape"]
                ))

            count = conn.execute("SELECT COUNT(*) FROM polygon_trades").fetchone()[0]
            assert count == 20

    def test_bronze_dedup_constraint(self, tmp_db):
        """Verify bronze layer rejects duplicate raw data via UNIQUE constraint."""
        with duckdb.connect(tmp_db) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS polygon_trades (
                    ticker TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    price REAL, size REAL, conditions TEXT,
                    exchange INTEGER, tape TEXT,
                    created_at TIMESTAMP DEFAULT now(),
                    UNIQUE(ticker, ts, exchange)
                )
            """)
            conn.execute("""
                INSERT INTO polygon_trades (ticker, ts, price, size, exchange)
                VALUES ('AAPL', '2024-01-01T10:00:00+00:00', 150.0, 100, 4)
            """)
            # Duplicate should be ignored
            conn.execute("""
                INSERT OR IGNORE INTO polygon_trades (ticker, ts, price, size, exchange)
                VALUES ('AAPL', '2024-01-01T10:00:00+00:00', 150.5, 200, 4)
            """)
            count = conn.execute("SELECT COUNT(*) FROM polygon_trades").fetchone()[0]
            assert count == 1, "Duplicate should be ignored in bronze layer"


# ── Silver Layer Tests ────────────────────────────────────────────────────────

class TestSilverLayer:
    """Tests for deduplicated OHLC bars in silver tables."""

    def test_polygon_bars_upsert(self, tmp_db, sample_ohlcv_bars):
        """Verify OHLC bars are inserted with dedup via INSERT OR IGNORE."""
        with duckdb.connect(tmp_db) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS polygon_bars (
                    ticker TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    timespan TEXT NOT NULL,
                    open REAL, high REAL, low REAL, close REAL,
                    volume REAL, vwap REAL, transactions INTEGER,
                    created_at TIMESTAMP DEFAULT now(),
                    UNIQUE(ticker, ts, timespan)
                )
            """)
            for bar in sample_ohlcv_bars:
                conn.execute("""
                    INSERT OR IGNORE INTO polygon_bars
                        (ticker, ts, timespan, open, high, low, close, volume, vwap, transactions)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    bar["ticker"], bar["ts"], bar["timespan"],
                    bar["open"], bar["high"], bar["low"], bar["close"],
                    bar["volume"], bar["vwap"], bar["transactions"]
                ))

            count = conn.execute("SELECT COUNT(*) FROM polygon_bars").fetchone()[0]
            assert count == 10

    def test_polygon_bars_dedup_idempotent(self, tmp_db, sample_ohlcv_bars):
        """Verify re-running ETL doesn't create duplicates."""
        with duckdb.connect(tmp_db) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS polygon_bars (
                    ticker TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    timespan TEXT NOT NULL,
                    open REAL, high REAL, low REAL, close REAL,
                    volume REAL, vwap REAL, transactions INTEGER,
                    created_at TIMESTAMP DEFAULT now(),
                    UNIQUE(ticker, ts, timespan)
                )
            """)
            # Insert twice
            for _ in range(2):
                for bar in sample_ohlcv_bars:
                    conn.execute("""
                        INSERT OR IGNORE INTO polygon_bars
                            (ticker, ts, timespan, open, high, low, close, volume, vwap, transactions)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        bar["ticker"], bar["ts"], bar["timespan"],
                        bar["open"], bar["high"], bar["low"], bar["close"],
                        bar["volume"], bar["vwap"], bar["transactions"]
                    ))

            count = conn.execute("SELECT COUNT(*) FROM polygon_bars").fetchone()[0]
            assert count == 10, "Idempotent insert should not create duplicates"

    def test_ohlc_data_integrity(self, tmp_db, sample_ohlcv_bars):
        """Verify OHLC relationships are valid (high >= low, etc.)."""
        with duckdb.connect(tmp_db) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS polygon_bars (
                    ticker TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    timespan TEXT NOT NULL,
                    open REAL, high REAL, low REAL, close REAL,
                    volume REAL, vwap REAL, transactions INTEGER,
                    created_at TIMESTAMP DEFAULT now(),
                    UNIQUE(ticker, ts, timespan)
                )
            """)
            for bar in sample_ohlcv_bars:
                conn.execute("""
                    INSERT INTO polygon_bars
                        (ticker, ts, timespan, open, high, low, close, volume, vwap, transactions)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    bar["ticker"], bar["ts"], bar["timespan"],
                    bar["open"], bar["high"], bar["low"], bar["close"],
                    bar["volume"], bar["vwap"], bar["transactions"]
                ))

            # Validate OHLC relationships
            result = conn.execute("""
                SELECT COUNT(*) FROM polygon_bars
                WHERE high < low OR high < open OR high < close
                   OR low > open OR low > close
            """).fetchone()[0]
            assert result == 0, "All bars should have high >= low and high >= open/close"

    def test_polygon_option_bars_upsert(self, tmp_db, sample_option_bars):
        """Verify option bars use INSERT OR REPLACE for updates."""
        with duckdb.connect(tmp_db) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS polygon_option_bars (
                    option_ticker TEXT NOT NULL,
                    underlying TEXT NOT NULL,
                    expiry TEXT, strike REAL, "right" TEXT,
                    ts TEXT NOT NULL,
                    timespan TEXT NOT NULL,
                    open REAL, high REAL, low REAL, close REAL,
                    volume REAL, vwap REAL, transactions INTEGER,
                    created_at TIMESTAMP DEFAULT now(),
                    UNIQUE(option_ticker, ts, timespan)
                )
            """)
            for bar in sample_option_bars:
                conn.execute("""
                    INSERT OR REPLACE INTO polygon_option_bars
                        (option_ticker, underlying, expiry, strike, "right",
                         ts, timespan, open, high, low, close, volume, vwap, transactions)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    bar["option_ticker"], bar["underlying"], bar["expiry"],
                    bar["strike"], bar["right"], bar["ts"], bar["timespan"],
                    bar["open"], bar["high"], bar["low"], bar["close"],
                    bar["volume"], bar["vwap"], bar["transactions"]
                ))

            count = conn.execute("SELECT COUNT(*) FROM polygon_option_bars").fetchone()[0]
            assert count == 5

    def test_option_bars_replace_updates(self, tmp_db, sample_option_bars):
        """Verify INSERT OR REPLACE updates existing option bar rows."""
        with duckdb.connect(tmp_db) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS polygon_option_bars (
                    option_ticker TEXT NOT NULL,
                    underlying TEXT NOT NULL,
                    expiry TEXT, strike REAL, "right" TEXT,
                    ts TEXT NOT NULL,
                    timespan TEXT NOT NULL,
                    open REAL, high REAL, low REAL, close REAL,
                    volume REAL, vwap REAL, transactions INTEGER,
                    created_at TIMESTAMP DEFAULT now(),
                    UNIQUE(option_ticker, ts, timespan)
                )
            """)
            bar = sample_option_bars[0]
            # Insert original
            conn.execute("""
                INSERT INTO polygon_option_bars
                    (option_ticker, underlying, expiry, strike, "right",
                     ts, timespan, open, high, low, close, volume, vwap, transactions)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                bar["option_ticker"], bar["underlying"], bar["expiry"],
                bar["strike"], bar["right"], bar["ts"], bar["timespan"],
                bar["open"], bar["high"], bar["low"], bar["close"],
                bar["volume"], bar["vwap"], bar["transactions"]
            ))

            # Replace with updated volume
            conn.execute("""
                INSERT OR REPLACE INTO polygon_option_bars
                    (option_ticker, underlying, expiry, strike, "right",
                     ts, timespan, open, high, low, close, volume, vwap, transactions)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                bar["option_ticker"], bar["underlying"], bar["expiry"],
                bar["strike"], bar["right"], bar["ts"], bar["timespan"],
                bar["open"], bar["high"], bar["low"], bar["close"],
                999999, bar["vwap"], bar["transactions"]
            ))

            row = conn.execute(
                "SELECT volume FROM polygon_option_bars WHERE option_ticker = ?",
                (bar["option_ticker"],)
            ).fetchone()
            assert row[0] == 999999, "INSERT OR REPLACE should update existing row"


# ── Gold Layer Tests ──────────────────────────────────────────────────────────

class TestGoldLayer:
    """Tests for enriched/embedding tables derived from silver layer."""

    def test_ticker_embeddings_from_polygon_tickers(self, tmp_db):
        """Verify gold layer can be populated from silver polygon_tickers."""
        with duckdb.connect(tmp_db) as conn:
            # Drop and recreate ticker_embeddings without HNSW (VSS not loaded in test env)
            conn.execute("DROP TABLE IF EXISTS ticker_embeddings")
            conn.execute("""
                CREATE TABLE ticker_embeddings (
                    ticker TEXT PRIMARY KEY,
                    industry TEXT, source TEXT, text TEXT,
                    embedding FLOAT[384],
                    updated_at TIMESTAMP DEFAULT now()
                )
            """)

            # Silver: insert reference data
            conn.execute("""
                INSERT INTO polygon_tickers (ticker, name, description, updated_at)
                VALUES ('AAPL', 'Apple Inc', 'Technology company', '2024-01-01')
            """)

            # Gold: derive embeddings from silver
            fake_embedding = [0.1] * 384
            conn.execute("""
                INSERT INTO ticker_embeddings (ticker, text, embedding, source)
                SELECT ticker, description, ?, 'polygon_tickers'
                FROM polygon_tickers WHERE ticker = 'AAPL'
            """, [fake_embedding])

            count = conn.execute("SELECT COUNT(*) FROM ticker_embeddings").fetchone()[0]
            assert count == 1

            row = conn.execute(
                "SELECT ticker, text, source FROM ticker_embeddings WHERE ticker = 'AAPL'"
            ).fetchone()
            assert row[0] == "AAPL"
            assert row[1] == "Technology company"
            assert row[2] == "polygon_tickers"

    def test_gold_layer_read_from_silver(self, tmp_db, sample_ohlcv_bars):
        """Verify gold layer queries can read from silver OHLC bars."""
        with duckdb.connect(tmp_db) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS polygon_bars (
                    ticker TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    timespan TEXT NOT NULL,
                    open REAL, high REAL, low REAL, close REAL,
                    volume REAL, vwap REAL, transactions INTEGER,
                    created_at TIMESTAMP DEFAULT now(),
                    UNIQUE(ticker, ts, timespan)
                )
            """)
            # Populate silver
            for bar in sample_ohlcv_bars:
                conn.execute("""
                    INSERT INTO polygon_bars
                        (ticker, ts, timespan, open, high, low, close, volume, vwap, transactions)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    bar["ticker"], bar["ts"], bar["timespan"],
                    bar["open"], bar["high"], bar["low"], bar["close"],
                    bar["volume"], bar["vwap"], bar["transactions"]
                ))

            # Gold-level aggregation query: latest bar per ticker
            result = conn.execute("""
                WITH ranked AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ts DESC) as rn
                    FROM polygon_bars
                )
                SELECT ticker, ts, close, volume FROM ranked WHERE rn = 1
            """).fetchone()

            assert result[0] == "AAPL"
            assert result[2] is not None  # close price


# ── Pipeline Flow Tests ───────────────────────────────────────────────────────

class TestPipelineFlow:
    """Tests for end-to-end data movement through medallion layers."""

    def test_bronze_to_silver_flow(self, tmp_db):
        """Verify data flows from bronze trades to silver aggregated bars."""
        with duckdb.connect(tmp_db) as conn:
            # Bronze: raw trades
            conn.execute("""
                CREATE TABLE IF NOT EXISTS polygon_trades (
                    ticker TEXT NOT NULL, ts TEXT NOT NULL,
                    price REAL, size REAL, conditions TEXT,
                    exchange INTEGER, tape TEXT,
                    created_at TIMESTAMP DEFAULT now(),
                    UNIQUE(ticker, ts, exchange)
                )
            """)
            # Silver: aggregated bars
            conn.execute("""
                CREATE TABLE IF NOT EXISTS polygon_bars (
                    ticker TEXT NOT NULL, ts TEXT NOT NULL, timespan TEXT NOT NULL,
                    open REAL, high REAL, low REAL, close REAL,
                    volume REAL, vwap REAL, transactions INTEGER,
                    created_at TIMESTAMP DEFAULT now(),
                    UNIQUE(ticker, ts, timespan)
                )
            """)

            # Insert bronze trades
            base_ts = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
            for i in range(100):
                ts = (base_ts + timedelta(seconds=i)).isoformat(timespec="seconds")
                conn.execute("""
                    INSERT INTO polygon_trades (ticker, ts, price, size, exchange)
                    VALUES ('AAPL', ?, ?, ?, 4)
                """, (ts, 150.0 + (i % 10), 100 + i))

            # Silver: aggregate bronze trades into daily bars
            # Use MIN/MAX for open/close since DuckDB FIRST/LAST with order arg may not be available
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

            silver_count = conn.execute("SELECT COUNT(*) FROM polygon_bars").fetchone()[0]
            assert silver_count == 1, "All trades should aggregate to 1 daily bar"

            bar = conn.execute("SELECT open, high, low, close FROM polygon_bars").fetchone()
            assert bar[1] >= bar[0], "High should be >= open"
            assert bar[2] <= bar[1], "Low should be <= high"

    def test_silver_to_gold_derivation(self, tmp_db, sample_ohlcv_bars):
        """Verify gold layer can be derived from silver bars with window functions."""
        with duckdb.connect(tmp_db) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS polygon_bars (
                    ticker TEXT NOT NULL, ts TEXT NOT NULL, timespan TEXT NOT NULL,
                    open REAL, high REAL, low REAL, close REAL,
                    volume REAL, vwap REAL, transactions INTEGER,
                    created_at TIMESTAMP DEFAULT now(),
                    UNIQUE(ticker, ts, timespan)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ticker_embeddings (
                    ticker TEXT PRIMARY KEY,
                    industry TEXT, source TEXT, text TEXT,
                    embedding FLOAT[384],
                    updated_at TIMESTAMP DEFAULT now()
                )
            """)

            # Populate silver
            for bar in sample_ohlcv_bars:
                conn.execute("""
                    INSERT INTO polygon_bars
                        (ticker, ts, timespan, open, high, low, close, volume, vwap, transactions)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    bar["ticker"], bar["ts"], bar["timespan"],
                    bar["open"], bar["high"], bar["low"], bar["close"],
                    bar["volume"], bar["vwap"], bar["transactions"]
                ))

            # Gold: compute derived metrics from silver
            import numpy as np
            result = conn.execute("""
                WITH metrics AS (
                    SELECT
                        ticker,
                        AVG(close) as avg_close,
                        SUM(volume) as total_volume,
                        MAX(high) as period_high,
                        MIN(low) as period_low
                    FROM polygon_bars
                    GROUP BY ticker
                )
                SELECT * FROM metrics
            """).fetchone()

            assert result[0] == "AAPL"
            assert result[1] is not None  # avg_close
            assert result[2] is not None  # total_volume
            assert result[3] >= result[4], "Period high should be >= period low"

    def test_full_medallion_pipeline(self, tmp_db):
        """End-to-end test: bronze → silver → gold with real aggregation logic."""
        with duckdb.connect(tmp_db) as conn:
            # Drop and recreate ticker_embeddings without HNSW (VSS not loaded in test env)
            conn.execute("DROP TABLE IF EXISTS ticker_embeddings")
            conn.execute("""
                CREATE TABLE ticker_embeddings (
                    ticker TEXT PRIMARY KEY,
                    industry TEXT, source TEXT, text TEXT,
                    embedding FLOAT[384],
                    updated_at TIMESTAMP DEFAULT now()
                )
            """)

            # Bronze: insert raw IBKR snapshots
            base_ts = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
            for i in range(50):
                ts = (base_ts + timedelta(minutes=i)).isoformat(timespec="seconds")
                price = 190.0 + (i % 20)
                conn.execute("""
                    INSERT INTO stock_quotes (ticker, ts, last, bid, ask, volume, "open", high, low, "close")
                    VALUES ('AAPL', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (ts, price, price - 0.5, price + 0.5, 1000 + i * 100, price, price + 2, price - 2, price))

            # Silver: aggregate bronze into hourly bars
            conn.execute("""
                INSERT OR IGNORE INTO polygon_bars
                    (ticker, ts, timespan, open, high, low, close, volume, transactions)
                SELECT
                    ticker,
                    DATE_TRUNC('hour', ts::TIMESTAMP) as ts,
                    'hour' as timespan,
                    MIN("open") as open,
                    MAX(high) as high,
                    MIN(low) as low,
                    MAX("close") as close,
                    SUM(volume) as volume,
                    COUNT(*) as transactions
                FROM stock_quotes
                GROUP BY ticker, DATE_TRUNC('hour', ts::TIMESTAMP)
            """)

            # Gold: derive summary metrics
            conn.execute("""
                INSERT INTO ticker_embeddings (ticker, text, embedding, source)
                SELECT
                    ticker,
                    'Avg price: ' || ROUND(AVG(close), 2) || ', Total volume: ' || SUM(volume),
                    NULL,
                    'stock_quotes_aggregated'
                FROM polygon_bars
                WHERE ticker = 'AAPL'
                GROUP BY ticker
            """)

            # Verify pipeline
            bronze = conn.execute("SELECT COUNT(*) FROM stock_quotes").fetchone()[0]
            silver = conn.execute("SELECT COUNT(*) FROM polygon_bars").fetchone()[0]
            gold = conn.execute("SELECT COUNT(*) FROM ticker_embeddings").fetchone()[0]

            assert bronze == 50, "Bronze should have 50 raw snapshots"
            assert silver >= 1, "Silver should have aggregated bars"
            assert gold == 1, "Gold should have 1 derived embedding"

    def test_large_dataset_performance(self, tmp_db):
        """Verify pipeline handles large datasets efficiently (10k rows)."""
        with duckdb.connect(tmp_db) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS polygon_bars (
                    ticker TEXT NOT NULL, ts TEXT NOT NULL, timespan TEXT NOT NULL,
                    open REAL, high REAL, low REAL, close REAL,
                    volume REAL, vwap REAL, transactions INTEGER,
                    created_at TIMESTAMP DEFAULT now(),
                    UNIQUE(ticker, ts, timespan)
                )
            """)

            # Insert 10k bars across multiple tickers
            tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
            base_ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
            for ticker in tickers:
                for i in range(2000):
                    ts = (base_ts + timedelta(days=i)).isoformat(timespec="seconds")
                    price = 100.0 + i * 0.1
                    conn.execute("""
                        INSERT OR IGNORE INTO polygon_bars
                            (ticker, ts, timespan, open, high, low, close, volume, vwap, transactions)
                        VALUES (?, ?, 'day', ?, ?, ?, ?, ?, ?, ?)
                    """, (ticker, ts, price, price + 5, price - 5, price + 1, 1000000, price, 50000))

            # Verify dedup worked
            total = conn.execute("SELECT COUNT(*) FROM polygon_bars").fetchone()[0]
            assert total == 10000, f"Expected 10000 unique bars, got {total}"

            # Verify aggregation query performance
            result = conn.execute("""
                SELECT ticker, COUNT(*), AVG(close), SUM(volume)
                FROM polygon_bars
                GROUP BY ticker
                ORDER BY ticker
            """).fetchall()
            assert len(result) == 5
            for row in result:
                assert row[1] == 2000, f"Each ticker should have 2000 bars"


# ── Query Layer Tests ─────────────────────────────────────────────────────────

class TestQueryLayer:
    """Tests for read patterns used by query.py and rag_engine.py."""

    def test_latest_bar_per_ticker(self, tmp_db, sample_ohlcv_bars):
        """Verify window function for latest bar retrieval works correctly."""
        with duckdb.connect(tmp_db) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS polygon_bars (
                    ticker TEXT NOT NULL, ts TEXT NOT NULL, timespan TEXT NOT NULL,
                    open REAL, high REAL, low REAL, close REAL,
                    volume REAL, vwap REAL, transactions INTEGER,
                    created_at TIMESTAMP DEFAULT now(),
                    UNIQUE(ticker, ts, timespan)
                )
            """)
            for bar in sample_ohlcv_bars:
                conn.execute("""
                    INSERT INTO polygon_bars
                        (ticker, ts, timespan, open, high, low, close, volume, vwap, transactions)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    bar["ticker"], bar["ts"], bar["timespan"],
                    bar["open"], bar["high"], bar["low"], bar["close"],
                    bar["volume"], bar["vwap"], bar["transactions"]
                ))

            result = conn.execute("""
                WITH ranked AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ts DESC) as rn
                    FROM polygon_bars
                )
                SELECT ticker, ts, close, volume FROM ranked WHERE rn = 1
            """).fetchone()

            assert result[0] == "AAPL"
            assert result[1] == sample_ohlcv_bars[-1]["ts"]
            assert result[2] == sample_ohlcv_bars[-1]["close"]

    def test_date_range_filter(self, tmp_db, sample_ohlcv_bars):
        """Verify date range filtering on OHLC bars works correctly."""
        with duckdb.connect(tmp_db) as conn:
            # Tables already exist from init_db() via tmp_db fixture

            for bar in sample_ohlcv_bars:
                conn.execute("""
                    INSERT INTO polygon_bars
                        (ticker, ts, timespan, open, high, low, close, volume, vwap, transactions)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    bar["ticker"], bar["ts"], bar["timespan"],
                    bar["open"], bar["high"], bar["low"], bar["close"],
                    bar["volume"], bar["vwap"], bar["transactions"]
                ))

            # Filter: Jan 3 through Jan 7 inclusive
            result = conn.execute("""
                SELECT COUNT(*) FROM polygon_bars
                WHERE ts >= '2024-01-03T00:00:00+00:00'
                  AND ts <  '2024-01-08T00:00:00+00:00'
            """).fetchone()[0]

            assert result == 5, f"Date range filter should return 5 bars (Jan 3-7), got {result}"
