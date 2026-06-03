"""
db/vector_store.py
DuckDB + VSS vector store for embedding-based similarity search.

Stores text embeddings (384-dim, all-MiniLM-L6-v2) in an HNSW index.
Designed to support multiple sources: polygon descriptions, EDGAR filings, etc.
"""
import os
import duckdb
from pathlib import Path
from loguru import logger


DUCKDB_PATH   = os.getenv("DUCKDB_PATH", "./data/vectors.duckdb")
EMBEDDING_DIM = 384   # all-MiniLM-L6-v2 output dimension


def get_duck_connection() -> duckdb.DuckDBPyConnection:
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute("LOAD vss")
    return conn


def init_vector_db():
    """Install the VSS extension, create tables and HNSW index."""
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(DUCKDB_PATH)

    conn.execute("INSTALL vss")
    conn.execute("LOAD vss")

    # DuckDB <1.1 requires this pragma for persistent HNSW indexes
    try:
        conn.execute("SET hnsw_enable_experimental_persistence = true")
    except Exception:
        pass   # not needed in DuckDB 1.1+

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS ticker_embeddings (
            ticker      VARCHAR PRIMARY KEY,
            source      VARCHAR NOT NULL,   -- 'polygon_desc' | 'edgar_filing' | ...
            text        VARCHAR,            -- original text that was embedded
            embedding   FLOAT[{EMBEDDING_DIM}],
            embedded_at VARCHAR
        )
    """)

    # HNSW index — CREATE INDEX doesn't support IF NOT EXISTS for VSS,
    # so check manually before creating
    existing = conn.execute("""
        SELECT index_name FROM duckdb_indexes()
        WHERE table_name = 'ticker_embeddings'
    """).fetchall()

    if not any(r[0] == "idx_ticker_hnsw" for r in existing):
        conn.execute(f"""
            CREATE INDEX idx_ticker_hnsw
            ON ticker_embeddings USING HNSW (embedding)
            WITH (metric = 'cosine')
        """)
        logger.info("Created HNSW index on ticker_embeddings")

    conn.close()
    logger.info(f"Vector DB initialised at {DUCKDB_PATH}")
