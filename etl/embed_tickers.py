"""
etl/embed_tickers.py
Generates text embeddings for ticker descriptions and stores them in DuckDB.

Current source: polygon_tickers.description
Future sources: EDGAR filings, news, etc. — add a new run_embed_<source>_etl()
and upsert into ticker_embeddings with a different source tag.
"""
from datetime import datetime, timezone
from typing import List, Optional

from loguru import logger

from db.database import get_connection
from db.vector_store import EMBEDDING_DIM, get_duck_connection

_model = None   # lazy-loaded SentenceTransformer


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
    Read descriptions from polygon_tickers (SQLite), embed with
    all-MiniLM-L6-v2, and upsert into DuckDB ticker_embeddings.
    Returns number of tickers embedded.
    """
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT ticker, name, description
            FROM polygon_tickers
            WHERE description IS NOT NULL AND trim(description) != ''
        """).fetchall()

    if not rows:
        logger.warning("No ticker descriptions found — run --job polygon-ref first")
        return 0

    model  = _get_model()
    duck   = get_duck_connection()
    total  = 0
    ts     = _utcnow()

    for i in range(0, len(rows), batch_size):
        batch  = rows[i : i + batch_size]
        texts  = [f"{r['name']}: {r['description']}" for r in batch]
        vecs   = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)

        for j, row in enumerate(batch):
            duck.execute("""
                INSERT OR REPLACE INTO ticker_embeddings
                    (ticker, source, text, embedding, embedded_at)
                VALUES (?, 'polygon_desc', ?, ?, ?)
            """, [row["ticker"], texts[j], vecs[j].tolist(), ts])

        total += len(batch)
        logger.debug(f"Embedded {total}/{len(rows)} tickers")

    duck.commit()
    duck.close()
    logger.info(f"Embedding ETL complete: {total} tickers")
    return total


# ── Similarity search ─────────────────────────────────────────────────────────

def search_similar_tickers(query: str, top_k: int = 10) -> List[tuple]:
    """
    Find tickers whose descriptions are most similar to a natural-language query.
    Returns list of (ticker, text, distance) — lower distance = more similar.

    Example:
        search_similar_tickers("semiconductor AI chip company", top_k=5)
    """
    model = _get_model()
    qvec  = model.encode([query], normalize_embeddings=True)[0].tolist()

    duck  = get_duck_connection()
    rows  = duck.execute(f"""
        SELECT ticker, text,
               array_distance(embedding, ?::FLOAT[{EMBEDDING_DIM}]) AS distance
        FROM ticker_embeddings
        ORDER BY distance ASC
        LIMIT ?
    """, [qvec, top_k]).fetchall()
    duck.close()
    return rows


# ── Helper ────────────────────────────────────────────────────────────────────

def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
