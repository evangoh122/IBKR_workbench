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
            vwap        REAL,
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
            und_price       REAL,
            pv_dividend     REAL,
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

    # ── Polygon: OHLCV bars ───────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS polygon_bars (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker          TEXT    NOT NULL,
            ts              TEXT    NOT NULL,   -- bar open time, ISO-8601 UTC
            timespan        TEXT    NOT NULL,   -- 'day' | 'minute' | 'hour'
            open            REAL,
            high            REAL,
            low             REAL,
            close           REAL,
            volume          REAL,
            vwap            REAL,
            transactions    INTEGER,
            created_at      TEXT    DEFAULT (datetime('now')),
            UNIQUE(ticker, ts, timespan)
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_pb_ticker_ts
            ON polygon_bars(ticker, ts, timespan)
    """)

    # ── Polygon: real-time / delayed snapshots ────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS polygon_snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT    NOT NULL,
            ts          TEXT    NOT NULL,
            bid         REAL,
            ask         REAL,
            last        REAL,
            prev_close  REAL,
            day_volume  REAL,
            created_at  TEXT    DEFAULT (datetime('now'))
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ps_ticker_ts
            ON polygon_snapshots(ticker, ts)
    """)

    # ── Polygon: options chain snapshots ──────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS polygon_option_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            underlying      TEXT    NOT NULL,
            expiry          TEXT    NOT NULL,   -- YYYY-MM-DD
            strike          REAL    NOT NULL,
            right           TEXT    NOT NULL,   -- 'call' | 'put'
            ts              TEXT    NOT NULL,
            day_open        REAL,
            day_close       REAL,
            day_volume      INTEGER,
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
        CREATE INDEX IF NOT EXISTS idx_pos_underlying
            ON polygon_option_snapshots(underlying, expiry, strike, right)
    """)

    # ── Polygon: ticker reference / metadata ──────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS polygon_tickers (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker           TEXT    NOT NULL UNIQUE,
            name             TEXT,
            market           TEXT,
            primary_exchange TEXT,
            type             TEXT,
            active           INTEGER,
            currency         TEXT,
            description      TEXT,
            updated_at       TEXT    NOT NULL
        )
    """)

    conn.commit()
    conn.close()
    logger.info(f"Database initialised at {DB_PATH}")
