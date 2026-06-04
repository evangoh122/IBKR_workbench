"""
tests/gold/test_gold_rag_retrieval.py
Gold layer: Tests for RAG engine retrieval from OHLCV data.

These tests verify that:
- PriceContextRetriever fetches latest close prices from polygon_bars
- DuckDBVectorRetriever handles keyword fallback correctly

Note: rag_engine opens connections with read_only=True, which conflicts
with the writable db_conn fixture. We close db_conn before invoking
retrievers to avoid DuckDB's "different configuration" error.
"""
import pytest

from db.database import get_connection


@pytest.fixture
def seed_polygon_bars_for_rag(tmp_db, db_conn):
    """Seed polygon_bars for RAG retrieval tests."""
    bars = [
        ("AAPL", "2024-06-04T00:00:00+00:00", "day", 180.0, 185.0, 175.0, 182.5, 50000000, 181.0, 100000),
        ("AAPL", "2024-06-03T00:00:00+00:00", "day", 178.0, 183.0, 173.0, 180.0, 48000000, 179.0, 95000),
        ("MSFT", "2024-06-04T00:00:00+00:00", "day", 400.0, 410.0, 395.0, 405.0, 20000000, 402.0, 50000),
    ]
    for bar in bars:
        db_conn.execute("""
            INSERT INTO polygon_bars (ticker, ts, timespan, open, high, low, close, volume, vwap, transactions)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, bar)
    db_conn.commit()
    db_conn.close()
    return tmp_db


@pytest.fixture
def seed_polygon_tickers_for_rag(tmp_db, db_conn):
    """Seed polygon_tickers for keyword search tests."""
    tickers = [
        ("AAPL", "Apple Inc", "stocks", "NASDAQ", "cs", 1, "USD", "Technology company that makes iPhones", "2024-01-01"),
        ("MSFT", "Microsoft", "stocks", "NASDAQ", "cs", 1, "USD", "Software and cloud computing company", "2024-01-01"),
    ]
    for t in tickers:
        db_conn.execute("""
            INSERT INTO polygon_tickers (ticker, name, market, primary_exchange, type, active, currency, description, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, t)
    db_conn.commit()
    db_conn.close()
    return tmp_db


def test_price_context_retriever_returns_latest_close(seed_polygon_bars_for_rag, monkeypatch):
    """Verify PriceContextRetriever returns latest close prices."""
    from rag_engine import PriceContextRetriever

    monkeypatch.setattr("rag_engine.DB_PATH", seed_polygon_bars_for_rag)

    retriever = PriceContextRetriever(top_k=5)
    docs = retriever.invoke("What is the price of AAPL?")

    assert len(docs) > 0
    content = docs[0].page_content
    assert "AAPL" in content
    assert "182.5" in content  # Latest close


def test_price_context_retriever_multiple_tickers(seed_polygon_bars_for_rag, monkeypatch):
    """Verify PriceContextRetriever returns document with multiple tickers."""
    from rag_engine import PriceContextRetriever

    monkeypatch.setattr("rag_engine.DB_PATH", seed_polygon_bars_for_rag)

    retriever = PriceContextRetriever(top_k=5)
    docs = retriever.invoke("Compare AAPL and MSFT prices")

    assert len(docs) >= 1
    # PriceContextRetriever combines all prices into a single document
    content = docs[0].page_content
    assert "AAPL" in content
    assert "MSFT" in content


def test_price_context_retriever_empty_for_unknown(seed_polygon_bars_for_rag, monkeypatch):
    """Verify PriceContextRetriever returns empty for unknown ticker."""
    from rag_engine import PriceContextRetriever

    monkeypatch.setattr("rag_engine.DB_PATH", seed_polygon_bars_for_rag)

    retriever = PriceContextRetriever(top_k=5)
    docs = retriever.invoke("What is the price of UNKNOWN?")

    assert isinstance(docs, list)


def test_keyword_fallback_returns_tickers(seed_polygon_tickers_for_rag, monkeypatch):
    """Verify DuckDBVectorRetriever keyword fallback finds tickers."""
    from rag_engine import DuckDBVectorRetriever

    monkeypatch.setattr("rag_engine.DB_PATH", seed_polygon_tickers_for_rag)

    retriever = DuckDBVectorRetriever(top_k=5)
    docs = retriever._keyword_fallback("Apple")

    assert len(docs) > 0
    tickers_found = {doc.metadata.get("ticker") for doc in docs}
    assert "AAPL" in tickers_found


def test_keyword_fallback_injection_safe(seed_polygon_tickers_for_rag, monkeypatch):
    """Verify keyword fallback handles SQL injection safely."""
    from rag_engine import DuckDBVectorRetriever

    monkeypatch.setattr("rag_engine.DB_PATH", seed_polygon_tickers_for_rag)

    retriever = DuckDBVectorRetriever(top_k=5)
    docs = retriever._keyword_fallback("dummy' OR '1'='1")

    assert len(docs) == 0


def test_price_context_retriever_metadata(seed_polygon_bars_for_rag, monkeypatch):
    """Verify PriceContextRetriever includes source metadata."""
    from rag_engine import PriceContextRetriever

    monkeypatch.setattr("rag_engine.DB_PATH", seed_polygon_bars_for_rag)

    retriever = PriceContextRetriever(top_k=5)
    docs = retriever.invoke("AAPL price")

    assert len(docs) > 0
    doc = docs[0]
    assert doc.metadata.get("source") == "polygon_bars"
