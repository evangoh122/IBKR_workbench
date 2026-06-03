"""
Natural-language chat interface over the IBKR/Polygon/EDGAR DuckDB database.

Supported providers:
  CHAT_PROVIDER = deepseek | mimo
  CHAT_MODEL    = optional model override
"""
import os
import re
from typing import Optional

import duckdb
import pandas as pd
from loguru import logger
from openai import OpenAI

DB_PATH = os.getenv("DB_PATH", "./data/ibkr.duckdb")

_PROVIDERS = {
    "deepseek": {
        "base_url": "https://api.deepseek.com",
        "model": "deepseek-chat",
        "api_key_env": "DEEPSEEK_API_KEY",
        "allow_blank_key": False,
    },
    "mimo": {
        "base_url": os.getenv("MIMO_BASE_URL", "http://localhost:11434/v1"),
        "model": os.getenv("MIMO_MODEL", "xiaomi/MiMo-7B-RL"),
        "api_key_env": "MIMO_API_KEY",
        "allow_blank_key": True,
    },
    "openai": {
        "base_url": "https://api.openai.com/v1",
        "model": "gpt-4o",
        "api_key_env": "OPENAI_API_KEY",
        "allow_blank_key": False,
    },
    "anthropic": {
        "base_url": "https://api.anthropic.com/v1",  # used by rag_engine only; chat_engine raises ValueError for this provider
        "model": "claude-sonnet-4-6",
        "api_key_env": "ANTHROPIC_API_KEY",
        "allow_blank_key": False,
    },
    "ollama": {
        "base_url": os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1"),
        "model": os.getenv("OLLAMA_MODEL", "llama3.2"),
        "api_key_env": "OLLAMA_API_KEY",
        "allow_blank_key": True,
    },
}

_PROVIDER = os.getenv("CHAT_PROVIDER", "deepseek").lower()
_CFG = _PROVIDERS.get(_PROVIDER, _PROVIDERS["deepseek"])
_BASE_URL = _CFG["base_url"]
_MODEL = os.getenv("CHAT_MODEL") or _CFG["model"]
_KEY_ENV = _CFG["api_key_env"]

SCHEMA = """
You have access to a DuckDB financial database with these tables:

IBKR live data:
- stock_quotes(ticker, ts, bid, ask, last, close, open, high, low, volume, vwap, created_at)
- option_quotes(ticker, expiry, strike, right, ts, bid, ask, last, volume, open_interest, implied_vol, delta, gamma, theta, vega, und_price, pv_dividend)
- option_chains(ticker, expiry, strike, right, exchange, fetched_at)
- etl_runs(id, run_type, status, rows_written, started_at, finished_at, message)

Polygon historical data:
- polygon_bars(ticker, ts, timespan, open, high, low, close, volume, vwap, transactions)
- polygon_snapshots(ticker, ts, bid, ask, last, prev_close, day_volume)
- polygon_option_snapshots(underlying, expiry, strike, right, ts, day_open, day_close, day_volume, open_interest, implied_vol, delta, gamma, theta, vega)
- polygon_tickers(ticker, name, market, primary_exchange, type, active, currency, description)

SEC EDGAR financials:
- edgar_filings(ticker, cik, form_type, filed_date, accession_number, primary_doc)
- edgar_facts(ticker, cik, taxonomy, concept, label, unit, value, period_start, period_end, form_type, filed_date)

Notes:
- Use DuckDB SQL syntax.
- Dates are stored as TEXT in ISO-8601 format. Cast with ::TIMESTAMP or ::DATE as needed.
- Always LIMIT results to 100 rows unless the user asks for more.
- For latest queries use QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ts DESC) = 1.
"""

SYSTEM_PROMPT = f"""You are a financial data analyst assistant. The user will ask questions about their market data.

{SCHEMA}

Rules:
1. If the question requires data, respond with ONLY a valid DuckDB SQL query. No markdown, no explanation.
2. If the question is conversational or cannot be answered with SQL, respond with a plain English answer starting with "ANSWER:".
3. Never make up data. Only query what exists in the schema above.
4. Keep SQL readable and add brief inline comments for complex logic.
5. Only generate read-only SELECT or WITH queries.
"""


def _get_client() -> OpenAI:
    if _PROVIDER == "anthropic":
        raise ValueError(
            "Anthropic's API is not OpenAI-compatible and cannot be used with the SQL chat "
            "interface (which uses the OpenAI client). "
            "For SQL chat use CHAT_PROVIDER=deepseek, openai, ollama, or mimo. "
            "Anthropic is supported in RAG mode (rag_engine) via langchain-anthropic."
        )
    api_key = os.getenv(_KEY_ENV, "")
    if not api_key and not _CFG["allow_blank_key"]:
        raise ValueError(
            f"{_KEY_ENV} is not set in .env (CHAT_PROVIDER={_PROVIDER})."
        )
    return OpenAI(api_key=api_key or "local", base_url=_BASE_URL)


def chat(question: str, history: Optional[list] = None, max_rows: int = 100) -> dict:
    """
    Ask a natural-language question about the database.

    Returns a dict with type, sql, data, and answer fields.
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
            temperature=0.1,
            max_tokens=1024,
        )
        reply = response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"{_PROVIDER} API error: {e}")
        return {"type": "error", "sql": None, "data": None, "answer": f"API error: {e}"}

    if reply.startswith("ANSWER:"):
        return {
            "type": "text",
            "sql": None,
            "data": None,
            "answer": reply[len("ANSWER:"):].strip(),
        }

    sql = _clean_sql(reply)
    validation_error = _validate_read_only_sql(sql)
    if validation_error:
        return {"type": "error", "sql": sql, "data": None, "answer": validation_error}

    try:
        with duckdb.connect(DB_PATH, read_only=True) as conn:
            df = conn.execute(f"SELECT * FROM ({sql}) AS chat_result LIMIT ?", (max_rows,)).df()
    except Exception as e:
        logger.warning(f"SQL execution failed: {e}\nSQL: {sql}")
        return {"type": "error", "sql": sql, "data": None, "answer": f"SQL error: {e}"}

    answer = "The query returned no results." if df.empty else _summarise(client, question, df)
    return {"type": "table", "sql": sql, "data": df.head(max_rows), "answer": answer}


def _clean_sql(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
    return text.strip()


def _strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n\r]*", " ", sql)
    return sql.strip()


def _validate_read_only_sql(sql: str) -> Optional[str]:
    compact = _strip_sql_comments(sql)
    if not compact:
        return "The model did not return a SQL query."
    if ";" in compact:
        return "Rejected SQL with semicolons or multiple statements."

    first = compact.lstrip().split(None, 1)[0].lower()
    if first not in {"select", "with"}:
        return "Rejected SQL because only SELECT and WITH queries are allowed."

    blocked = {
        "attach", "call", "copy", "create", "delete", "detach", "drop",
        "export", "from_csv", "glob", "httpfs", "import", "insert",
        "install", "load", "pragma", "read_blob", "read_csv", "read_json",
        "read_parquet", "read_text", "set", "update",
    }
    tokens = set(re.findall(r"\b[a-z_][a-z0-9_]*\b", compact.lower()))
    found = sorted(tokens & blocked)
    if found:
        return f"Rejected SQL containing blocked keyword/function: {', '.join(found)}."
    return None


def _summarise(client: OpenAI, question: str, df: pd.DataFrame) -> str:
    preview = df.head(5).to_markdown(index=False)
    try:
        resp = client.chat.completions.create(
            model=_MODEL,
            messages=[{
                "role": "user",
                "content": (
                    f'The user asked: "{question}"\n\n'
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
