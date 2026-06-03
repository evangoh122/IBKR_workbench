"""
etl/embed_tickers.py
Generates text embeddings for ticker descriptions and stores them in DuckDB.

Current source: polygon_tickers.description
"""
from datetime import datetime, timezone
from typing import List, Optional

from loguru import logger

from db.database import get_connection

_model = None   # lazy-loaded SentenceTransformer
EMBEDDING_DIM = 384


def _get_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        logger.info("Loading embedding model all-MiniLM-L6-v2 (first run downloads ~90 MB)…")
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model


# ── Embedding ETL ─────────────────────────────────────────────────────────────

def run_embed_tickers_etl(batch_size: int = 64) -> int:
    """
    Read descriptions from polygon_tickers (DuckDB), embed with
    all-MiniLM-L6-v2, and upsert into DuckDB ticker_embeddings.
    Returns number of tickers embedded.
    """
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT ticker, name, description
            FROM polygon_tickers
            WHERE description IS NOT NULL AND trim(description) != ''
        """).df().to_dict('records')

    if not rows:
        logger.warning("No ticker descriptions found — run --job polygon-ref first")
        return 0

    model  = _get_model()
    total  = 0
    ts     = _utcnow()

    with get_connection() as conn:
        for i in range(0, len(rows), batch_size):
            batch  = rows[i : i + batch_size]
            texts  = [f"{r['name']}: {r['description']}" for r in batch]
            vecs   = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)

            for j, row in enumerate(batch):
                conn.execute("""
                    INSERT OR REPLACE INTO ticker_embeddings
                        (ticker, source, text, embedding, updated_at)
                    VALUES (?, 'polygon_desc', ?, ?, ?)
                """, [row["ticker"], texts[j], vecs[j].tolist(), ts])

            total += len(batch)
            logger.debug(f"Embedded {total}/{len(rows)} tickers")
        conn.commit()

    logger.info(f"Embedding ETL complete: {total} tickers")
    return total


# ── Helper ────────────────────────────────────────────────────────────────────

def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
