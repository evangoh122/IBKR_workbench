"""
db/database.py
SQLite schema + connection manager for IBKR ETL.
"""
import sqlite3
import os
from pathlib import Path
from loguru import logger


DB_PATH = os.getenv("DB_PATH", "./data/ibkr.db")


def get_connection() -> sqlite3.Connection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create all tables if they don't exist."""
    conn = get_connection()
    cur = conn.cursor()

    # ── Stocks ────────────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_quotes (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT    NOT NULL,
            ts          TEXT    NOT NULL,          -- ISO-8601 UTC
            bid         REAL,
            ask         REAL,
            last        REAL,
            close       REAL,
            volume      INTEGER,
            open        REAL,
            high        REAL,
            low         REAL,
            created_at  TEXT    DEFAULT (datetime('now'))
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_sq_ticker_ts
            ON stock_quotes(ticker, ts)
    """)

    # ── Options ───────────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS option_quotes (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker          TEXT    NOT NULL,   -- underlying
            expiry          TEXT    NOT NULL,   -- YYYYMMDD
            strike          REAL    NOT NULL,
            right           TEXT    NOT NULL,   -- 'C' or 'P'
            ts              TEXT    NOT NULL,
            bid             REAL,
            ask             REAL,
            last            REAL,
            volume          INTEGER,
            open_interest   INTEGER,
            implied_vol     REAL,
            delta           REAL,
            gamma           REAL,
            theta           REAL,
            vega            REAL,
            created_at      TEXT    DEFAULT (datetime('now'))
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_oq_ticker_expiry
            ON option_quotes(ticker, expiry, strike, right)
    """)

    # ── Option Chains (metadata) ───────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS option_chains (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT    NOT NULL,
            expiry      TEXT    NOT NULL,
            strike      REAL    NOT NULL,
            right       TEXT    NOT NULL,
            exchange    TEXT,
            fetched_at  TEXT    DEFAULT (datetime('now')),
            UNIQUE(ticker, expiry, strike, right)
        )
    """)

    # ── ETL Run Log ────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_runs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_type    TEXT    NOT NULL,   -- 'stocks' | 'options' | 'chain'
            status      TEXT    NOT NULL,   -- 'ok' | 'error'
            message     TEXT,
            rows_written INTEGER DEFAULT 0,
            started_at  TEXT    NOT NULL,
            finished_at TEXT
        )
    """)

    conn.commit()
    conn.close()
    logger.info(f"Database initialised at {DB_PATH}")
