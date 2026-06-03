"""
rag_engine.py
Retrieval-Augmented Generation using LangChain and DuckDB.
Queries `edgar_embeddings` and `ticker_embeddings` for context before
sending to the LLM.
"""
import os
from typing import List
from loguru import logger
import duckdb

from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from langchain_core.callbacks import CallbackManagerForRetrieverRun
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI

from etl.embed_tickers import _get_model
from etl.chat_engine import _PROVIDER, _BASE_URL, _MODEL, _KEY_ENV
from db.database import get_connection

class DuckDBVectorRetriever(BaseRetriever):
    """Custom Retriever for DuckDB VSS (array_distance)."""
    top_k: int = 5
    
    def _get_relevant_documents(self, query: str, *, run_manager: CallbackManagerForRetrieverRun) -> List[Document]:
        model = _get_model()
        qvec = model.encode([query], normalize_embeddings=True, show_progress_bar=False)[0].tolist()
        
        docs = []
        with get_connection() as conn:
            # 1. Search EDGAR 10-K Filings
            edgar_rows = conn.execute(f"""
                SELECT ticker, text, array_distance(embedding, ?::FLOAT[384]) as distance
                FROM edgar_embeddings
                ORDER BY distance ASC
                LIMIT ?
            """, [qvec, self.top_k]).df().to_dict('records')
            
            for row in edgar_rows:
                docs.append(Document(
                    page_content=row['text'],
                    metadata={"source": "10-K Filing", "ticker": row['ticker'], "distance": row['distance']}
                ))
                
            # 2. Search Ticker Descriptions
            desc_rows = conn.execute(f"""
                SELECT ticker, text, array_distance(embedding, ?::FLOAT[384]) as distance
                FROM ticker_embeddings
                ORDER BY distance ASC
                LIMIT ?
            """, [qvec, self.top_k]).df().to_dict('records')
            
            for row in desc_rows:
                docs.append(Document(
                    page_content=row['text'],
                    metadata={"source": "Company Description", "ticker": row['ticker'], "distance": row['distance']}
                ))
        
        # Sort combined results by distance
        docs = sorted(docs, key=lambda d: d.metadata['distance'])
        return docs[:self.top_k]

def build_rag_chain():
    """Build LCEL chain for RAG."""
    if _KEY_ENV is None:
        api_key = "ollama"
    else:
        api_key = os.getenv(_KEY_ENV, "")
        if not api_key:
            logger.warning(f"RAG: {_KEY_ENV} is not set. Using dummy key.")
            api_key = "dummy"

    llm = ChatOpenAI(
        model=_MODEL,
        api_key=api_key,
        base_url=_BASE_URL
    )
    
    retriever = DuckDBVectorRetriever(top_k=5)
    
    template = """Answer the question based ONLY on the following context. If you cannot answer based on the context, say "I don't know based on the provided context."

Context:
{context}

Question: {question}

Answer:"""
    
    prompt = ChatPromptTemplate.from_template(template)
    
    def format_docs(docs: List[Document]):
        formatted = []
        for d in docs:
            formatted.append(f"[Source: {d.metadata['source']} | Ticker: {d.metadata['ticker']}]\n{d.page_content}")
        return "\n\n".join(formatted)

    rag_chain = (
        {"context": retriever | format_docs, "question": RunnablePassthrough()}
        | prompt
        | llm
        | StrOutputParser()
    )
    return rag_chain

def ask_rag(question: str) -> str:
    """Entry point to query the RAG engine."""
    logger.info(f"RAG Query: {question}")
    chain = build_rag_chain()
    try:
        response = chain.invoke(question)
        return response
    except Exception as e:
        logger.error(f"RAG failed: {e}")
        return f"Error executing RAG: {e}"

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    q = "What is the primary business of Apple?"
    print(ask_rag(q))
