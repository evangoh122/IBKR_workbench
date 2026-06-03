"""
etl/chat_engine.py
Natural-language chat interface over the IBKR/Polygon/EDGAR DuckDB database.

Supports DeepSeek and Xiaomi MiMo (or any OpenAI-compatible endpoint).
Configure via .env:
  CHAT_PROVIDER = deepseek | mimo | custom
  DEEPSEEK_API_KEY = ...
  MIMO_API_KEY     = ...          # MiMo hosted endpoint key
  MIMO_BASE_URL    = ...          # e.g. http://localhost:11434/v1 for Ollama
  CHAT_MODEL       = override default model name
"""
import os
from typing import Optional

import duckdb
import pandas as pd
from openai import OpenAI
from loguru import logger

DB_PATH = os.getenv("DB_PATH", "./data/ibkr.duckdb")

# ── Provider config ───────────────────────────────────────────────────────────
_PROVIDERS = {
    "deepseek": {
        "base_url":  "https://api.deepseek.com",
        "model":     "deepseek-chat",       # or "deepseek-reasoner" for R1
        "api_key_env": "DEEPSEEK_API_KEY",
    },
    "mimo": {
        "base_url":  os.getenv("MIMO_BASE_URL", "http://localhost:11434/v1"),
        "model":     "xiaomi/MiMo-7B-RL",   # Ollama model name
        "api_key_env": "MIMO_API_KEY",       # set to "ollama" for local Ollama
    },
}

_PROVIDER  = os.getenv("CHAT_PROVIDER", "deepseek").lower()
_CFG       = _PROVIDERS.get(_PROVIDER, _PROVIDERS["deepseek"])
_BASE_URL  = os.getenv("MIMO_BASE_URL" if _PROVIDER == "mimo" else "DEEPSEEK_BASE_URL",
                        _CFG["base_url"])
_MODEL     = os.getenv("CHAT_MODEL", _CFG["model"])
_KEY_ENV   = _CFG["api_key_env"]

# ── Schema context fed to the LLM ────────────────────────────────────────────

SCHEMA = """
You have access to a DuckDB financial database with these tables:

**IBKR live data**
- stock_quotes(ticker, ts, bid, ask, last, close, open, high, low, volume, vwap, created_at)
  Live IBKR stock snapshots. ts is ISO-8601 UTC.
- option_quotes(ticker, expiry, strike, right, ts, bid, ask, last, volume, open_interest, implied_vol, delta, gamma, theta, vega, und_price, pv_dividend)
  right is 'C' (call) or 'P' (put). expiry is YYYYMMDD.
- option_chains(ticker, expiry, strike, right, exchange, fetched_at)
  Metadata: all available option contracts per ticker.
- etl_runs(id, run_type, status, rows_written, started_at, finished_at, message)
  ETL job audit log.

**Polygon historical data**
- polygon_bars(ticker, ts, timespan, open, high, low, close, volume, vwap, transactions)
  Daily OHLCV+VWAP bars. timespan='day' for daily data.
- polygon_snapshots(ticker, ts, bid, ask, last, prev_close, day_volume)
  Delayed/EOD stock snapshots.
- polygon_option_snapshots(underlying, expiry, strike, right, ts, day_open, day_close, day_volume, open_interest, implied_vol, delta, gamma, theta, vega)
- polygon_tickers(ticker, name, market, primary_exchange, type, active, currency, description)
  Reference data: company names, exchanges, descriptions.

**SEC EDGAR financials**
- edgar_filings(ticker, cik, form_type, filed_date, accession_number, primary_doc)
  10-K / 10-Q / 8-K filing history.
- edgar_facts(ticker, cik, taxonomy, concept, label, unit, value, period_start, period_end, form_type, filed_date)
  XBRL financial facts. Key concepts: 'Revenues', 'NetIncomeLoss', 'EarningsPerShareDiluted',
  'Assets', 'Liabilities', 'StockholdersEquity', 'CashAndCashEquivalentsAtCarryingValue'.

**Notes**
- Use DuckDB SQL syntax (not SQLite). Use QUALIFY for window deduplication.
- Dates are stored as TEXT in ISO-8601 format. Cast with ::TIMESTAMP or ::DATE as needed.
- Always LIMIT results to 100 rows unless the user asks for more.
- For "latest" queries use: QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ts DESC) = 1
"""

SYSTEM_PROMPT = f"""You are a financial data analyst assistant. The user will ask questions about their market data.

{SCHEMA}

Rules:
1. If the question requires data, respond with ONLY a valid DuckDB SQL query — no markdown, no explanation.
2. If the question is conversational or cannot be answered with SQL, respond with a plain English answer starting with "ANSWER:".
3. Never make up data. Only query what exists in the schema above.
4. Keep SQL readable and add brief inline comments for complex logic.
"""


# ── Client ────────────────────────────────────────────────────────────────────

def _get_client() -> OpenAI:
    api_key = os.getenv(_KEY_ENV, "ollama")   # "ollama" works for local Ollama
    if not api_key:
        raise ValueError(f"{_KEY_ENV} is not set in .env (CHAT_PROVIDER={_PROVIDER})")
    return OpenAI(api_key=api_key, base_url=_BASE_URL)


# ── Core chat function ────────────────────────────────────────────────────────

def chat(
    question: str,
    history: Optional[list] = None,
    max_rows: int = 100,
) -> dict:
    """
    Ask a natural-language question about the database.

    Returns:
        {
            "type":    "table" | "text" | "error",
            "sql":     str | None,
            "data":    pd.DataFrame | None,
            "answer":  str,
        }
    """
    client = _get_client()

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    if history:
        messages.extend(history)
    messages.append({"role": "user", "content": question})

    try:
        response = client.chat.completions.create(
            model=_MODEL,
            messages=messages,
            temperature=0.1,   # low temp for deterministic SQL
            max_tokens=1024,
        )
        reply = response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"DeepSeek API error: {e}")
        return {"type": "error", "sql": None, "data": None, "answer": f"API error: {e}"}

    # Plain-text answer (non-SQL response)
    if reply.startswith("ANSWER:"):
        return {
            "type":   "text",
            "sql":    None,
            "data":   None,
            "answer": reply[len("ANSWER:"):].strip(),
        }

    # SQL response — execute it
    sql = _clean_sql(reply)
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        df   = conn.execute(sql).df()
        conn.close()

        if df.empty:
            answer = "The query returned no results."
        else:
            answer = _summarise(client, question, df)

        return {"type": "table", "sql": sql, "data": df.head(max_rows), "answer": answer}

    except Exception as e:
        logger.warning(f"SQL execution failed: {e}\nSQL: {sql}")
        return {"type": "error", "sql": sql, "data": None, "answer": f"SQL error: {e}"}


def _clean_sql(text: str) -> str:
    """Strip markdown code fences if the model added them."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text  = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
    return text.strip()


def _summarise(client: OpenAI, question: str, df: pd.DataFrame) -> str:
    """Ask DeepSeek to write a one-sentence plain-English summary of the results."""
    preview = df.head(5).to_markdown(index=False)
    try:
        resp = client.chat.completions.create(
            model=_MODEL,
            messages=[{
                "role": "user",
                "content": (
                    f"The user asked: \"{question}\"\n\n"
                    f"Query returned {len(df)} rows. Here are the first 5:\n{preview}\n\n"
                    "Write a concise 1-2 sentence plain-English answer. No markdown."
                ),
            }],
            temperature=0.3,
            max_tokens=200,
        )
        return resp.choices[0].message.content.strip()
    except Exception:
        return f"Query returned {len(df)} rows."
