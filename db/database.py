"""
db/database.py
DuckDB schema + connection manager for IBKR ETL.
"""
import duckdb
import os
from pathlib import Path
from loguru import logger


DB_PATH = os.getenv("DB_PATH", "./data/ibkr.duckdb")


def get_connection() -> duckdb.DuckDBPyConnection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(DB_PATH)
    # ── Extension Setup ───────────────────────────────────────────
    try:
        conn.execute("INSTALL vss;")
        conn.execute("LOAD vss;")
        conn.execute("SET hnsw_enable_experimental_persistence = true;")
    except Exception as e:
        logger.warning(f"Failed to load VSS extension: {e}")
    return conn


def init_db():
    """Create all tables if they don't exist."""
    with get_connection() as conn:
        # ── Stocks ────────────────────────────────────────────────────────────────
        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS stock_quotes_id_seq;
            CREATE TABLE IF NOT EXISTS stock_quotes (
                id          INTEGER PRIMARY KEY DEFAULT nextval('stock_quotes_id_seq'),
                ticker      TEXT    NOT NULL,
                ts          TEXT    NOT NULL,          -- ISO-8601 UTC
                bid         REAL,
                ask         REAL,
                last        REAL,
                "close"     REAL,
                volume      INTEGER,
                "open"      REAL,
                high        REAL,
                low         REAL,
                vwap        REAL,
                created_at  TIMESTAMP DEFAULT now()
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sq_ticker_ts
                ON stock_quotes(ticker, ts)
        """)

        # ── Options ───────────────────────────────────────────────────────────────
        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS option_quotes_id_seq;
            CREATE TABLE IF NOT EXISTS option_quotes (
                id              INTEGER PRIMARY KEY DEFAULT nextval('option_quotes_id_seq'),
                ticker          TEXT    NOT NULL,   -- underlying
                expiry          TEXT    NOT NULL,   -- YYYYMMDD
                strike          REAL    NOT NULL,
                "right"         TEXT    NOT NULL,   -- 'C' or 'P'
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
                created_at      TIMESTAMP DEFAULT now()
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_oq_ticker_expiry
                ON option_quotes(ticker, expiry, strike, "right")
        """)

        # ── Option Chains (metadata) ───────────────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS option_chains (
                ticker      TEXT    NOT NULL,
                expiry      TEXT    NOT NULL,
                strike      REAL    NOT NULL,
                "right"     TEXT    NOT NULL,
                exchange    TEXT,
                fetched_at  TIMESTAMP DEFAULT now(),
                UNIQUE(ticker, expiry, strike, "right")
            )
        """)

        # ── ETL Run Log ────────────────────────────────────────────────────────
        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS etl_runs_id_seq;
            CREATE TABLE IF NOT EXISTS etl_runs (
                id          INTEGER PRIMARY KEY DEFAULT nextval('etl_runs_id_seq'),
                run_type    TEXT    NOT NULL,   -- 'stocks' | 'options' | 'chain'
                status      TEXT    NOT NULL,   -- 'ok' | 'error'
                message     TEXT,
                rows_written INTEGER DEFAULT 0,
                started_at  TEXT    NOT NULL,
                finished_at TEXT
            )
        """)

        # ── Polygon: OHLCV bars ───────────────────────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS polygon_bars (
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
                created_at      TIMESTAMP DEFAULT now(),
                UNIQUE(ticker, ts, timespan)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_pb_ticker_ts
                ON polygon_bars(ticker, ts, timespan)
        """)

        # ── Polygon: real-time / delayed snapshots ────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS polygon_snapshots (
                ticker      TEXT    NOT NULL,
                ts          TEXT    NOT NULL,
                bid         REAL,
                ask         REAL,
                last        REAL,
                prev_close  REAL,
                day_volume  REAL,
                created_at  TIMESTAMP DEFAULT now()
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ps_ticker_ts
                ON polygon_snapshots(ticker, ts)
        """)

        # ── Polygon: options chain snapshots ──────────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS polygon_option_snapshots (
                underlying      TEXT    NOT NULL,
                expiry          TEXT    NOT NULL,   -- YYYY-MM-DD
                strike          REAL    NOT NULL,
                "right"         TEXT    NOT NULL,   -- 'call' | 'put'
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
                created_at      TIMESTAMP DEFAULT now()
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_pos_underlying
                ON polygon_option_snapshots(underlying, expiry, strike, "right")
        """)

        # ── Polygon: ticker reference / metadata ──────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS polygon_tickers (
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

        # ── EDGAR: filing metadata ────────────────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS edgar_filings (
                ticker           TEXT    NOT NULL,
                cik              TEXT    NOT NULL,
                form_type        TEXT    NOT NULL,
                filed_date       TEXT,
                accession_number TEXT    NOT NULL,
                primary_doc      TEXT,
                created_at       TIMESTAMP DEFAULT now(),
                UNIQUE(accession_number)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ef_ticker_form
                ON edgar_filings(ticker, form_type, filed_date)
        """)

        # ── EDGAR: XBRL financial facts ───────────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS edgar_facts (
                ticker           TEXT    NOT NULL,
                cik              TEXT    NOT NULL,
                taxonomy         TEXT    NOT NULL,
                concept          TEXT    NOT NULL,
                label            TEXT,
                unit             TEXT,
                value            REAL,
                period_start     TEXT,
                period_end       TEXT,
                form_type        TEXT,
                filed_date       TEXT,
                accession_number TEXT,
                created_at       TIMESTAMP DEFAULT now(),
                UNIQUE(ticker, concept, unit, period_end, form_type)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_edgar_facts_ticker
                ON edgar_facts(ticker, concept, period_end)
        """)

        # ── Vector Storage ────────────────────────────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ticker_embeddings (
                ticker      TEXT PRIMARY KEY,
                industry    TEXT,
                source      TEXT,
                text        TEXT,
                embedding   FLOAT[384],    -- all-MiniLM-L6-v2 dimension
                updated_at  TIMESTAMP DEFAULT now()
            )
        """)
        try:
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ticker_emb 
                ON ticker_embeddings USING HNSW (embedding) 
                WITH (metric = 'cosine')
            """)
        except Exception as e:
            logger.warning(f"Failed to create HNSW index: {e}")

    logger.info(f"Database initialised at {DB_PATH}")
