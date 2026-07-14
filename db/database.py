"""
db/database.py
DuckDB schema + connection manager for Equity Workbench ETL.
"""
import duckdb
import os
from pathlib import Path
from loguru import logger


DB_PATH = os.getenv("DB_PATH", "./data/equity.duckdb")


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
    conn = get_connection()
    try:
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
                run_type    TEXT    NOT NULL,   -- 'stocks' | 'options' | 'chain' | 'polygon_bars_bronze' | ...
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

        # ── Polygon: historical options OHLCV bars ───────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS polygon_option_bars (
                option_ticker   TEXT    NOT NULL,   -- e.g. O:AAPL240119C00150000
                underlying      TEXT    NOT NULL,
                expiry          TEXT,               -- YYYY-MM-DD
                strike          REAL,
                "right"         TEXT,               -- 'call' | 'put'
                ts              TEXT    NOT NULL,   -- bar open time, ISO-8601 UTC
                timespan        TEXT    NOT NULL,   -- 'day' | 'minute'
                open            REAL,
                high            REAL,
                low             REAL,
                close           REAL,
                volume          REAL,
                vwap            REAL,
                transactions    INTEGER,
                created_at      TIMESTAMP DEFAULT now(),
                UNIQUE(option_ticker, ts, timespan)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_pob_underlying
                ON polygon_option_bars(underlying, expiry, strike, "right")
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_pob_ticker_ts
                ON polygon_option_bars(option_ticker, ts)
        """)

        # ── Polygon: individual trade ticks ──────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS polygon_trades (
                ticker      TEXT    NOT NULL,
                ts          TEXT    NOT NULL,   -- SIP timestamp, ISO-8601 microsecond UTC
                price       REAL,
                size        REAL,
                conditions  TEXT,               -- comma-separated condition codes
                exchange    INTEGER,
                tape        TEXT,
                created_at  TIMESTAMP DEFAULT now(),
                UNIQUE(ticker, ts, exchange)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ptrades_ticker_ts
                ON polygon_trades(ticker, ts)
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

        # ── COT: Commitments of Traders (CFTC) ────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cot_reports (
                market_name     TEXT    NOT NULL,
                ticker          TEXT,               -- Optional mapping to IBKR ticker
                report_date     TEXT    NOT NULL,   -- ISO-8601
                noncomm_long    INTEGER,
                noncomm_short   INTEGER,
                comm_long       INTEGER,
                comm_short      INTEGER,
                total_long      INTEGER,
                total_short     INTEGER,
                noncomm_spreads INTEGER,
                open_interest   INTEGER,
                created_at      TIMESTAMP DEFAULT now(),
                UNIQUE(market_name, report_date)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_cot_market_date
                ON cot_reports(market_name, report_date)
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
            logger.warning(f"Failed to create HNSW index on ticker_embeddings: {e}")

        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS edgar_embeddings_id_seq;
            CREATE TABLE IF NOT EXISTS edgar_embeddings (
                id          INTEGER PRIMARY KEY DEFAULT nextval('edgar_embeddings_id_seq'),
                ticker      TEXT,
                accession   TEXT,
                text        TEXT,
                embedding   FLOAT[384],
                updated_at  TIMESTAMP DEFAULT now()
            )
        """)
        try:
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_edgar_emb
                ON edgar_embeddings USING HNSW (embedding)
                WITH (metric = 'cosine')
            """)
        except Exception as e:
            logger.warning(f"Failed to create HNSW index on edgar_embeddings: {e}")

        # ══ SILVER LAYER ═══════════════════════════════════════════════════════
        # Derived, recomputable feature tables built from bronze bars.
        # Grain: one row per entity per trading day. Rebuilt with INSERT OR REPLACE.

        # ── Silver: per-stock daily technical features ─────────────────────────
        # Source: polygon_bars WHERE timespan='day'. Windows are trailing N days.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_stock_features (
                ticker        TEXT    NOT NULL,
                ts            TEXT    NOT NULL,   -- trading day, ISO-8601 (bronze join key)
                trade_date    DATE,              -- typed date for window ordering
                close         DOUBLE,            -- from bronze polygon_bars (day)
                volume        DOUBLE,
                daily_return  DOUBLE,            -- close / prev_close - 1
                -- simple moving averages of close
                ma_20         DOUBLE,
                ma_50         DOUBLE,
                ma_100        DOUBLE,
                -- rolling sample standard deviation of close
                std_20        DOUBLE,
                std_50        DOUBLE,
                std_100       DOUBLE,
                pct_change    DOUBLE,            -- daily_return * 100 (percent)
                -- price z-score = (close - ma_N) / std_N
                zscore_20     DOUBLE,
                zscore_50     DOUBLE,
                zscore_100    DOUBLE,
                -- sigma band flag on price: '+3s' | 'normal' | '-3s' | NULL
                sigma_flag_20  TEXT,
                sigma_flag_50  TEXT,
                sigma_flag_100 TEXT,
                -- return z-score = (pct_change - mean_ret_N) / std_ret_N
                zscore_ret_20  DOUBLE,
                zscore_ret_50  DOUBLE,
                zscore_ret_100 DOUBLE,
                -- sigma band flag on returns
                sigma_flag_ret_20  TEXT,
                sigma_flag_ret_50  TEXT,
                sigma_flag_ret_100 TEXT,
                -- rolling VWAP = sum(typical_price*volume)/sum(volume), typical=(h+l+c)/3
                vwap_20       DOUBLE,
                vwap_50       DOUBLE,
                vwap_100      DOUBLE,
                computed_at   TIMESTAMP DEFAULT now(),
                UNIQUE(ticker, ts)
            )
        """)
        # ── Migrate: add sigma_flag columns if they don't exist yet ───────────
        for col in ("sigma_flag_20", "sigma_flag_50", "sigma_flag_100"):
            try:
                conn.execute(f"ALTER TABLE silver_stock_features ADD COLUMN {col} TEXT")
                logger.info(f"Migrated silver_stock_features: added {col}")
            except Exception:
                pass  # column already exists
        for col in ("pct_change",):
            try:
                conn.execute(f"ALTER TABLE silver_stock_features ADD COLUMN {col} DOUBLE")
                logger.info(f"Migrated silver_stock_features: added {col}")
            except Exception:
                pass
        for col in ("zscore_ret_20", "zscore_ret_50", "zscore_ret_100"):
            try:
                conn.execute(f"ALTER TABLE silver_stock_features ADD COLUMN {col} DOUBLE")
                logger.info(f"Migrated silver_stock_features: added {col}")
            except Exception:
                pass
        for col in ("sigma_flag_ret_20", "sigma_flag_ret_50", "sigma_flag_ret_100"):
            try:
                conn.execute(f"ALTER TABLE silver_stock_features ADD COLUMN {col} TEXT")
                logger.info(f"Migrated silver_stock_features: added {col}")
            except Exception:
                pass
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ssf_ticker_date
                ON silver_stock_features(ticker, trade_date)
        """)

        # ── View: latest z-score alerts per ticker ─────────────────────────────
        # Query this view to see which tickers are currently outside +-3 std devs.
        # sigma_flag values: '+3s' (above), '-3s' (below), 'normal', NULL (warm-up)
        conn.execute("DROP VIEW IF EXISTS v_zscore_alerts")
        conn.execute("""
            CREATE VIEW v_zscore_alerts AS
            WITH latest AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) AS rn
                FROM silver_stock_features
            )
            SELECT
                ticker,
                trade_date,
                close,
                pct_change,
                zscore_20,   sigma_flag_20,
                zscore_50,   sigma_flag_50,
                zscore_100,  sigma_flag_100,
                zscore_ret_20,  sigma_flag_ret_20,
                zscore_ret_50,  sigma_flag_ret_50,
                zscore_ret_100, sigma_flag_ret_100,
                CASE
                    WHEN sigma_flag_20      IN ('+3s', '-3s')
                      OR sigma_flag_50      IN ('+3s', '-3s')
                      OR sigma_flag_100     IN ('+3s', '-3s')
                      OR sigma_flag_ret_20  IN ('+3s', '-3s')
                      OR sigma_flag_ret_50  IN ('+3s', '-3s')
                      OR sigma_flag_ret_100 IN ('+3s', '-3s')
                    THEN true ELSE false
                END AS any_breach,
                GREATEST(
                    ABS(COALESCE(zscore_20,      0)),
                    ABS(COALESCE(zscore_50,      0)),
                    ABS(COALESCE(zscore_100,     0)),
                    ABS(COALESCE(zscore_ret_20,  0)),
                    ABS(COALESCE(zscore_ret_50,  0)),
                    ABS(COALESCE(zscore_ret_100, 0))
                ) AS max_abs_zscore
            FROM latest
            WHERE rn = 1
            ORDER BY max_abs_zscore DESC
        """)

        # ── Silver: per-contract daily option greeks (Black-Scholes-Merton) ────
        # Source: polygon_option_bars (option price) JOIN polygon_bars (underlying
        # close). implied_vol solved from the option's market close; greeks analytic.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_option_greeks (
                option_ticker   TEXT    NOT NULL,   -- OPRA symbol
                underlying      TEXT    NOT NULL,
                expiry          TEXT,               -- YYYY-MM-DD
                strike          DOUBLE,
                "right"         TEXT,               -- 'call' | 'put'
                ts              TEXT    NOT NULL,   -- trading day, ISO-8601
                trade_date      DATE,
                option_close    DOUBLE,             -- option price from bronze bar
                und_close       DOUBLE,             -- underlying close (S)
                time_to_expiry  DOUBLE,             -- years to expiry (ACT/365)
                moneyness       DOUBLE,             -- und_close / strike
                risk_free_rate  DOUBLE,             -- r assumption used
                dividend_yield  DOUBLE,             -- q assumption used (default 0)
                implied_vol     DOUBLE,             -- sigma solved from option_close
                delta           DOUBLE,
                gamma           DOUBLE,
                theta           DOUBLE,             -- per calendar day
                vega            DOUBLE,             -- per 1 vol point
                rho             DOUBLE,
                computed_at     TIMESTAMP DEFAULT now(),
                UNIQUE(option_ticker, ts)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sog_underlying_date
                
                ON silver_option_greeks(underlying, trade_date)
        """)

        # -- Silver: per-underlying daily options POSITIONING --
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_option_positioning (
                underlying      TEXT    NOT NULL,
                ts              TEXT    NOT NULL,
                trade_date      DATE,
                total_volume    DOUBLE,
                call_volume     DOUBLE,
                put_volume      DOUBLE,
                put_call_ratio  DOUBLE,
                atm_iv          DOUBLE,
                call_iv_25d     DOUBLE,
                put_iv_25d      DOUBLE,
                iv_skew_25d     DOUBLE,
                n_contracts     INTEGER,
                computed_at     TIMESTAMP DEFAULT now(),
                UNIQUE(underlying, ts)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sop_underlying_date
                ON silver_option_positioning(underlying, trade_date)
        """)

        # ── Silver: COT (Commitments of Traders) positioning features ─────────
        # Source: cot_reports (Bronze, weekly). Grain: one row per (ticker, report_date).
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_cot_features (
                report_date          DATE    NOT NULL,
                ticker               TEXT    NOT NULL,
                noncomm_long         BIGINT,
                noncomm_short        BIGINT,
                noncomm_net          BIGINT,
                comm_long            BIGINT,
                comm_short           BIGINT,
                comm_net             BIGINT,
                n_weeks              INTEGER,
                net_pos_mean_52w     DOUBLE,
                net_pos_std_52w      DOUBLE,
                net_pos_zscore_52w   DOUBLE,
                comm_net_mean_52w    DOUBLE,
                comm_net_std_52w     DOUBLE,
                comm_net_zscore_52w  DOUBLE,
                spec_comm_divergence DOUBLE,
                crowd_flag           TEXT,
                computed_at          TIMESTAMP DEFAULT now(),
                PRIMARY KEY (report_date, ticker)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_scf_ticker_date
                ON silver_cot_features(ticker, report_date)
        """)

        # ── Silver: futures continuous-contract price features ────────────────
        # Source: polygon_bars (Bronze, day bars) for continuous futures tickers
        # like 'ES1:COM'. Grain: one row per (ticker, trade_date).
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_futures_features (
                trade_date      DATE    NOT NULL,
                ticker          TEXT    NOT NULL,
                open_price      DOUBLE,
                high_price      DOUBLE,
                low_price       DOUBLE,
                close_price     DOUBLE,
                volume          BIGINT,
                n20             INTEGER,
                ma_20           DOUBLE,
                std_20          DOUBLE,
                zscore_20       DOUBLE,
                n_ret20         INTEGER,
                ret             DOUBLE,
                mean_ret_20     DOUBLE,
                std_ret_20      DOUBLE,
                zscore_ret_20   DOUBLE,
                vx_term_slope   DOUBLE,
                regime_flag     TEXT,
                computed_at     TIMESTAMP DEFAULT now(),
                PRIMARY KEY (trade_date, ticker)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sff_ticker_date
                ON silver_futures_features(ticker, trade_date)
        """)

        # ── View: latest COT positioning + futures price context per ticker ───
        conn.execute("DROP VIEW IF EXISTS v_cot_positioning")
        conn.execute("""
            CREATE VIEW v_cot_positioning AS
            WITH latest_cot AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY report_date DESC) AS rn
                FROM silver_cot_features
            ),
            latest_fut AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) AS rn
                FROM silver_futures_features
            )
            SELECT
                c.ticker,
                c.report_date,
                c.noncomm_net,
                c.net_pos_zscore_52w,
                c.comm_net_zscore_52w,
                c.spec_comm_divergence,
                c.crowd_flag,
                f.trade_date AS futures_date,
                f.close_price AS futures_close,
                f.zscore_20 AS futures_zscore_20,
                f.vx_term_slope,
                f.regime_flag
            FROM latest_cot c
            LEFT JOIN latest_fut f ON f.ticker = c.ticker || '1:COM' AND f.rn = 1
            WHERE c.rn = 1
            ORDER BY ABS(COALESCE(c.net_pos_zscore_52w, 0)) DESC
        """)

        # ══ GOLD LAYER ════════════════════
        # Backtest results: runs, trades, portfolio snapshots, metrics, signals.

        # ── Gold: backtest run metadata ───────────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_backtest_runs (
                run_id      TEXT NOT NULL PRIMARY KEY,
                config      TEXT,
                universe    TEXT,
                start_date  DATE NOT NULL,
                end_date    DATE NOT NULL,
                created_at  TIMESTAMP DEFAULT now(),
                fold_id     INTEGER            -- walk-forward OOS fold (NULL = plain run)
            )
        """)
        # ── Migrate: add fold_id if the table predates walk-forward ───────────
        try:
            conn.execute("ALTER TABLE gold_backtest_runs ADD COLUMN fold_id INTEGER")
            logger.info("Migrated gold_backtest_runs: added fold_id")
        except Exception:
            pass  # column already exists

        # ── Gold: individual simulated trades ─────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_trades (
                trade_id        TEXT    NOT NULL PRIMARY KEY,
                run_id          TEXT    NOT NULL,
                ticker          TEXT    NOT NULL,
                entry_date      DATE    NOT NULL,
                exit_date       DATE,
                direction       INTEGER NOT NULL,
                shares          DOUBLE,
                entry_price     DOUBLE,
                exit_price      DOUBLE,
                gross_pnl       DOUBLE,
                slippage_cost   DOUBLE,
                commission_cost DOUBLE,
                net_pnl         DOUBLE,
                entry_regime    TEXT,
                exit_regime     TEXT,
                signal_type     TEXT
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_gt_run_ticker
                ON gold_trades(run_id, ticker, entry_date)
        """)

        # ── Gold: daily portfolio snapshots ───────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_portfolio (
                run_id       TEXT NOT NULL,
                trade_date   DATE NOT NULL,
                nav          DOUBLE,
                cash         DOUBLE,
                drawdown_pct DOUBLE,
                n_positions  INTEGER,
                UNIQUE(run_id, trade_date)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_gp_run_date
                ON gold_portfolio(run_id, trade_date)
        """)

        # ── Gold: aggregated metrics per run ──────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_metrics (
                run_id            TEXT    NOT NULL PRIMARY KEY,
                sharpe            DOUBLE,
                sortino           DOUBLE,
                mdd_pct           DOUBLE,
                mdd_duration_days INTEGER,
                mdd_recovery_days INTEGER,
                calmar            DOUBLE,
                win_rate          DOUBLE,
                profit_factor     DOUBLE,
                expectancy        DOUBLE,
                cost_drag         DOUBLE,
                ann_return        DOUBLE,
                ann_vol           DOUBLE
            )
        """)

        # ── Gold: walk-forward OOS aggregate summary per walk-forward run ─────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_oos_summary (
                run_id TEXT NOT NULL,
                run_ts TIMESTAMPTZ DEFAULT current_timestamp,
                n_folds INTEGER,
                mean_sharpe DOUBLE,
                consistency_ratio DOUBLE,
                combined_sharpe DOUBLE,
                worst_fold_mdd DOUBLE,
                split_type TEXT,
                embargo_days INTEGER,
                PRIMARY KEY (run_id)
            )
        """)

        # ── Gold: immutable signal audit log ──────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_signals (
                signal_id   TEXT    NOT NULL PRIMARY KEY,
                run_id      TEXT    NOT NULL,
                ticker      TEXT    NOT NULL,
                signal_date DATE    NOT NULL,
                direction   INTEGER NOT NULL,
                strength    DOUBLE,
                signal_type TEXT,
                computed_at TIMESTAMP DEFAULT now()
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_gs_run_date
                ON gold_signals(run_id, signal_date, ticker)
        """)

    finally:
        conn.close()

    logger.info(f"Database initialised at {DB_PATH}")
