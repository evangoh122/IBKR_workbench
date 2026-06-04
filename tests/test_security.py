import os
from pathlib import Path
import duckdb
import pytest
import pandas as pd
from rag_engine import DuckDBVectorRetriever
from query import stock_history, latest_option_quotes

# Use a fixed file for testing instead of :memory: to avoid connection closing issues in mocks
# Use path relative to this test file to avoid CWD dependency
TEST_DB_FILE = str(Path(__file__).parent / "test_security.duckdb")

@pytest.fixture(scope="module")
def setup_test_db():
    if os.path.exists(TEST_DB_FILE):
        os.remove(TEST_DB_FILE)
    
    conn = duckdb.connect(TEST_DB_FILE)
    conn.execute("CREATE TABLE polygon_tickers (ticker TEXT, name TEXT, description TEXT)")
    conn.execute("INSERT INTO polygon_tickers VALUES ('AAPL', 'Apple Inc', 'Maker of iPhones')")
    conn.execute("INSERT INTO polygon_tickers VALUES ('MSFT', 'Microsoft', 'Windows and Cloud')")
    
    conn.execute("CREATE TABLE stock_quotes (ticker TEXT, ts TIMESTAMP, bid DOUBLE, ask DOUBLE, last DOUBLE, volume BIGINT, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE)")
    conn.execute("CREATE TABLE option_quotes (ticker TEXT, expiry TEXT, strike DOUBLE, \"right\" TEXT, ts TIMESTAMP, bid DOUBLE, ask DOUBLE, last DOUBLE, volume BIGINT)")
    conn.close()
    
    yield TEST_DB_FILE
    
    if os.path.exists(TEST_DB_FILE):
        os.remove(TEST_DB_FILE)

def test_rag_keyword_injection(monkeypatch, setup_test_db):
    """Verify that keyword search handles SQL injection payloads as literal text."""
    monkeypatch.setattr("rag_engine.DB_PATH", setup_test_db)
    
    retriever = DuckDBVectorRetriever(top_k=5)
    
    # Payload intended to bypass WHERE description IS NOT NULL and return everything
    injection_payload = "dummy' OR '1'='1"
    
    # If safe, it should return nothing (no ticker has this string in description).
    docs = retriever._keyword_fallback(injection_payload)
    
    assert len(docs) == 0, "Security Failure: SQL injection payload returned results in RAG!"

def test_query_stock_history_injection(monkeypatch, setup_test_db):
    """Verify stock_history parameterized query handles injection."""
    monkeypatch.setattr("query.DB_PATH", setup_test_db)
    
    # Payload: "AAPL' OR '1'='1"
    injection_ticker = "AAPL' OR '1'='1"
    df = stock_history(injection_ticker)
    
    assert df.empty, "Security Failure: stock_history injection payload returned results!"

def test_query_option_quotes_injection(monkeypatch, setup_test_db):
    """Verify latest_option_quotes parameterized query handles injection."""
    monkeypatch.setattr("query.DB_PATH", setup_test_db)
    
    # Payload in ticker
    df = latest_option_quotes("AAPL' OR '1'='1")
    assert df.empty
    
    # Payload in expiry
    df = latest_option_quotes("AAPL", expiry="2024-01-01' OR '1'='1")
    assert df.empty

if __name__ == "__main__":
    pytest.main([__file__])
