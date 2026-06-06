import os
import duckdb
from loguru import logger
from pathlib import Path

# Paths relative to project root
ROOT = Path(__file__).parent.parent
DB_PATH = os.getenv("DB_PATH", str(ROOT / "data" / "ibkr.duckdb"))
EMBEDDING_DIM = 768  # Gemini text-embedding-004 dimension

def migrate():
    if not Path(DB_PATH).exists():
        logger.error(f"Database not found at {DB_PATH}")
        return

    logger.info(f"Migrating {DB_PATH} to Gemini embeddings ({EMBEDDING_DIM} dims)...")
    
    conn = duckdb.connect(DB_PATH)
    try:
        # 1. Drop old tables
        logger.info("Dropping old embedding tables...")
        conn.execute("DROP TABLE IF EXISTS ticker_embeddings")
        conn.execute("DROP TABLE IF EXISTS edgar_embeddings")
        
        # 2. Recreate with new dimension
        logger.info(f"Creating new tables with {EMBEDDING_DIM} dimensions...")
        conn.execute(f"""
            CREATE TABLE ticker_embeddings (
                ticker TEXT PRIMARY KEY,
                source TEXT,
                text TEXT,
                industry TEXT,
                embedding FLOAT[{EMBEDDING_DIM}],
                updated_at TIMESTAMP DEFAULT now()
            )
        """)
        
        conn.execute(f"""
            CREATE TABLE edgar_embeddings (
                ticker TEXT,
                accession TEXT,
                text TEXT,
                embedding FLOAT[{EMBEDDING_DIM}],
                updated_at TIMESTAMP DEFAULT now()
            )
        """)
        
        logger.success("Migration successful! Tables are ready for Gemini embeddings.")
        logger.info("Note: You must now run 'python main.py --job embed-tickers' to re-populate.")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
