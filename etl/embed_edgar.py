"""
etl/embed_edgar.py
Downloads the most recent 10-K for each ticker from SEC EDGAR using sec-edgar-downloader,
extracts text using BeautifulSoup, splits into chunks via LangChain,
embeds with SentenceTransformers, and stores into DuckDB `edgar_embeddings`.
"""
import shutil
from pathlib import Path
from bs4 import BeautifulSoup
from typing import List
from datetime import datetime, timezone

from loguru import logger
from langchain_text_splitters import RecursiveCharacterTextSplitter
from sec_edgar_downloader import Downloader

from db.database import get_connection
from etl.embed_tickers import _get_embeddings as _get_model

# SEC requires a user-agent
_EMAIL = "research@example.com"
_COMPANY = "IBKR-Workbench"
_DOWNLOAD_DIR = Path("./data/edgar_downloads")

def fetch_latest_10k_with_downloader(ticker: str) -> str:
    """Download the latest 10-K using sec-edgar-downloader and return the file path to the primary HTML document."""
    dl = Downloader(_COMPANY, _EMAIL, _DOWNLOAD_DIR)
    
    # Download the latest 1 10-K filing
    try:
        num_downloaded = dl.get("10-K", ticker, limit=1, download_details=True)
        if num_downloaded == 0:
            return ""
            
        # The downloader creates a folder structure: ./data/edgar_downloads/sec-edgar-filings/<ticker>/10-K/<accession>/
        ticker_dir = _DOWNLOAD_DIR / "sec-edgar-filings" / ticker / "10-K"
        if not ticker_dir.exists():
            return ""
            
        # Find the accession folder (there should be only one since limit=1)
        accession_dirs = [d for d in ticker_dir.iterdir() if d.is_dir()]
        if not accession_dirs:
            return ""
            
        accession_dir = accession_dirs[0]
        
        # Look for primary-document.html (default name saved by the downloader)
        primary_doc_path = accession_dir / "primary-document.html"
        if primary_doc_path.exists():
            return str(primary_doc_path)
            
        return ""
    except Exception as e:
        logger.warning(f"Failed to download 10-K for {ticker}: {e}")
        return ""

def parse_html_file(file_path: str) -> str:
    """Read local HTML file and extract text."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
            
        soup = BeautifulSoup(content, "lxml")
        text = soup.get_text(separator="\n", strip=True)
        return text
    except Exception as e:
        logger.warning(f"Failed to parse {file_path}: {e}")
        return ""

def run_embed_edgar_etl(tickers: List[str]) -> int:
    """
    Main ETL job: use sec-edgar-downloader to fetch 10-Ks, chunk, embed, and store in DuckDB.
    """
    logger.info("Starting EDGAR embedding ETL with sec-edgar-downloader...")
    
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1500,
        chunk_overlap=150,
        length_function=len,
    )
    
    model = _get_model()
    total_chunks_stored = 0
    
    with get_connection() as conn:
        for ticker in tickers:
            logger.info(f"Processing 10-K for {ticker}...")
            
            file_path = fetch_latest_10k_with_downloader(ticker)
            
            if not file_path:
                logger.warning(f"No 10-K filing downloaded for {ticker}.")
                continue
                
            text = parse_html_file(file_path)
            
            if not text:
                continue
                
            chunks = text_splitter.split_text(text)
            logger.debug(f"{ticker}: Split into {len(chunks)} chunks.")
            
            # Embed in batches to save memory
            batch_size = 64
            ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
            accession = Path(file_path).parent.name
            
            for i in range(0, len(chunks), batch_size):
                batch_chunks = chunks[i : i + batch_size]
                vecs = model.encode(batch_chunks, normalize_embeddings=True, show_progress_bar=False)
                
                # Clear existing for this accession to ensure idempotency
                if i == 0:
                    conn.execute("DELETE FROM edgar_embeddings WHERE ticker = ? AND accession = ?", [ticker, accession])

                for j, chunk_text in enumerate(batch_chunks):
                    conn.execute("""
                        INSERT INTO edgar_embeddings
                            (ticker, accession, text, embedding, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, [ticker, accession, chunk_text, vecs[j].tolist(), ts])
                
                total_chunks_stored += len(batch_chunks)
            conn.commit()

    # Clean up downloaded files
    if _DOWNLOAD_DIR.exists():
        shutil.rmtree(_DOWNLOAD_DIR, ignore_errors=True)

    logger.info(f"EDGAR embedding complete. Stored {total_chunks_stored} total chunks.")
    return total_chunks_stored
