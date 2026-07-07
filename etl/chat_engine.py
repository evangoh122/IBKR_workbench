"""
Natural-language chat interface over the Equity Workbench/Polygon/EDGAR DuckDB database.

Supported providers:
  CHAT_PROVIDER = deepseek | mimo | openai | anthropic | ollama
  CHAT_MODEL    = optional model override
"""
import os
import re
from typing import Optional

import anthropic
import duckdb
import pandas as pd
from loguru import logger
from openai import OpenAI

DB_PATH = os.getenv("DB_PATH", "./data/equity.duckdb")

_PROVIDERS = {
    # Pipeline stage 1 — fast generation (Claude Haiku via Anthropic SDK)
    "mimo": {
        "sdk": "anthropic",
        "base_url": "https://api.anthropic.com",
        "model": "claude-haiku-4-5-20251001",
        "api_key_env": "ANTHROPIC_API_KEY",
        "allow_blank_key": False,
    },
    # Pipeline stage 2 — first review (Claude Sonnet via Anthropic SDK)
    "deepseek": {
        "sdk": "anthropic",
        "base_url": "https://api.anthropic.com",
        "model": "claude-sonnet-4-6",
        "api_key_env": "ANTHROPIC_API_KEY",
        "allow_blank_key": False,
    },
    # Pipeline stage 3 / single-provider default — final review (Claude Sonnet)
    "anthropic": {
        "sdk": "anthropic",
        "base_url": "https://api.anthropic.com",
        "model": "claude-sonnet-4-6",
        "api_key_env": "ANTHROPIC_API_KEY",
        "allow_blank_key": False,
    },
    # Native OpenAI (unchanged)
    "openai": {
        "sdk": "openai",
        "base_url": "https://api.openai.com/v1",
        "model": "gpt-4o",
        "api_key_env": "OPENAI_API_KEY",
        "allow_blank_key": False,
    },
    # Local Ollama (unchanged)
    "ollama": {
        "sdk": "openai",
        "base_url": os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1"),
        "model": os.getenv("OLLAMA_MODEL", "llama3.2"),
        "api_key_env": "OLLAMA_API_KEY",
        "allow_blank_key": True,
    },
}

_PROVIDER = os.getenv("CHAT_PROVIDER", "deepseek").lower()
_CFG = _PROVIDERS.get(_PROVIDER, _PROVIDERS["deepseek"])
_MODEL = os.getenv("CHAT_MODEL") or _CFG["model"]  # CHAT_MODEL env overrides provider default

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


def _call_provider(provider: str, messages: list, max_tokens: int = 1024, model: Optional[str] = None) -> str:
    """Call a specific provider by name and return the text reply."""
    cfg = _PROVIDERS.get(provider, _PROVIDERS["anthropic"])
    model = model or cfg["model"]
    api_key = os.getenv(cfg["api_key_env"], "")
    if not api_key and not cfg["allow_blank_key"]:
        raise ValueError(f"{cfg['api_key_env']} is not set in .env (provider={provider}).")

    if cfg.get("sdk", "openai") == "anthropic":
        client = anthropic.Anthropic(api_key=api_key)
        system = next((m["content"] for m in messages if m["role"] == "system"), "")
        user_msgs = [m for m in messages if m["role"] != "system"]
        resp = client.messages.create(
            model=model, max_tokens=max_tokens, system=system, messages=user_msgs,
        )
        text_block = next(b for b in resp.content if b.type == "text")
        return text_block.text
    else:
        client = OpenAI(api_key=api_key or "local", base_url=cfg["base_url"])
        resp = client.chat.completions.create(
            model=model, messages=messages, temperature=0.1, max_tokens=max_tokens,
        )
        return (resp.choices[0].message.content or "").strip()


def _call_llm(messages: list, max_tokens: int = 1024) -> str:
    """Send messages to the configured provider, respecting CHAT_MODEL override."""
    return _call_provider(_PROVIDER, messages, max_tokens, model=_MODEL)


_REVIEW_PROMPT = """You are reviewing a DuckDB SQL query generated by another model.
Check for:
1. Correctness — does it answer the user's question?
2. Safety — read-only SELECT/WITH only, no data mutation.
3. DuckDB syntax — valid functions, correct quoting.

Respond with one of:
- APPROVED: <brief reason>
- CORRECTED: <brief reason>
```sql
<corrected query>
```
"""


def chat_pipeline(question: str, history: Optional[list] = None, max_rows: int = 100) -> dict:
    """
    3-stage pipeline: MiMo generates SQL → DeepSeek reviews → Claude approves.

    Each stage can correct the SQL before passing it forward.
    Falls back gracefully if a review stage fails.
    """
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    if history:
        messages.extend(history)
    messages.append({"role": "user", "content": question})

    # Stage 1: MiMo generates SQL
    try:
        raw = _call_provider("mimo", messages, max_tokens=1024)
    except Exception as e:
        logger.error(f"MiMo generation failed: {e}")
        return {"type": "error", "sql": None, "data": None, "answer": f"Generation error: {e}"}

    if raw.startswith("ANSWER:"):
        return {"type": "text", "sql": None, "data": None, "answer": raw[len("ANSWER:"):].strip()}

    sql = _clean_sql(raw)

    def _extract_corrected(review: str) -> Optional[str]:
        m = re.search(r"```sql\n(.*?)\n```", review, re.DOTALL)
        return m.group(1).strip() if m else None

    # Stage 2: DeepSeek first review
    try:
        ds_review = _call_provider("deepseek", [
            {"role": "system", "content": _REVIEW_PROMPT},
            {"role": "user", "content": f"Question: {question}\n\nSQL:\n```sql\n{sql}\n```"},
        ], max_tokens=512)
        logger.info(f"DeepSeek review: {ds_review[:80]}")
        if ds_review.startswith("CORRECTED:"):
            corrected = _extract_corrected(ds_review)
            if corrected:
                sql = corrected
    except Exception as e:
        logger.warning(f"DeepSeek review skipped: {e}")

    # Stage 3: Claude final review
    try:
        claude_review = _call_provider("anthropic", [
            {"role": "system", "content": _REVIEW_PROMPT},
            {"role": "user", "content": f"Question: {question}\n\nSQL:\n```sql\n{sql}\n```"},
        ], max_tokens=512)
        logger.info(f"Claude review: {claude_review[:80]}")
        if claude_review.startswith("CORRECTED:"):
            corrected = _extract_corrected(claude_review)
            if corrected:
                sql = corrected
        elif not claude_review.startswith("APPROVED"):
            return {"type": "error", "sql": sql, "data": None, "answer": f"Review rejected: {claude_review}"}
    except Exception as e:
        logger.warning(f"Claude review skipped: {e}")

    validation_error = _validate_read_only_sql(sql)
    if validation_error:
        return {"type": "error", "sql": sql, "data": None, "answer": validation_error}

    try:
        with duckdb.connect(DB_PATH, read_only=True) as conn:
            try:
                conn.execute("SET enable_external_access=false")
            except Exception as _e:
                logger.debug(f"Could not set enable_external_access=false: {_e}")
            df = conn.execute(f"SELECT * FROM ({sql}) AS chat_result LIMIT ?", (max_rows,)).df()
    except Exception as e:
        logger.warning(f"SQL execution failed: {e}\nSQL: {sql}")
        return {"type": "error", "sql": sql, "data": None, "answer": f"SQL error: {e}"}

    answer = "The query returned no results." if df.empty else _summarise(question, df)
    return {"type": "table", "sql": sql, "data": df.head(max_rows), "answer": answer}


def chat(question: str, history: Optional[list] = None, max_rows: int = 100) -> dict:
    """
    Ask a natural-language question about the database.

    Returns a dict with type, sql, data, and answer fields.
    """
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    if history:
        messages.extend(history)
    messages.append({"role": "user", "content": question})

    try:
        reply = _call_llm(messages, max_tokens=1024)
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
            try:
                conn.execute("SET enable_external_access=false")
            except Exception as _e:
                logger.debug(f"Could not set enable_external_access=false: {_e}")
            df = conn.execute(f"SELECT * FROM ({sql}) AS chat_result LIMIT ?", (max_rows,)).df()
    except Exception as e:
        logger.warning(f"SQL execution failed: {e}\nSQL: {sql}")
        return {"type": "error", "sql": sql, "data": None, "answer": f"SQL error: {e}"}

    answer = "The query returned no results." if df.empty else _summarise(question, df)
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


def _summarise(question: str, df: pd.DataFrame) -> str:
    preview = df.head(5).to_markdown(index=False)
    try:
        return _call_llm([{
            "role": "user",
            "content": (
                f'The user asked: "{question}"\n\n'
                f"Query returned {len(df)} rows. Here are the first 5:\n{preview}\n\n"
                "Write a concise 1-2 sentence plain-English answer. No markdown."
            ),
        }], max_tokens=200)
    except Exception:
        return f"Query returned {len(df)} rows."
