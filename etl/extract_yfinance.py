"""
etl/extract_yfinance.py
yfinance-backed ETL for the staging area.

Two entry points:
  run_yf_bars_etl    — daily bars for the 32 validation tickers (Mag 7 + semis)
  run_yf_indices_etl — daily bars for 18 major indices + derived spreads + stats

All output lands in ibkr.duckdb under tables prefixed `staging_yf_`.
"""
import time
from typing import Iterable, Optional

import pandas as pd
import yfinance as yf
from loguru import logger

from db.database import get_connection
from etl.bulk_load_massive import TICKERS as SEMI_TICKERS


_RATE_DELAY = 0.5

INDICES = [
    "ACWI",   # MSCI All Country
    "ACWX",   # MSCI All Country ex-US
    "^GSPC",  # S&P 500
    "SPDW",   # S&P Developed World ex-US
    "RSP",    # S&P 500 Equal Weight
    "^DJI",   # Dow Jones Industrial Average
    "^IXIC",  # Nasdaq Composite
    "SPTM",   # S&P 1500 Total Market
    "MDY",    # S&P MidCap 400
    "^SP600", # S&P SmallCap 600
    "^RUT",   # Russell 2000
    "SMH",    # Semiconductor ETF
    "IGV",    # Software ETF
    "EZU",    # MSCI Europe
    "EEM",    # MSCI Emerging Markets
    "EWJ",    # MSCI Japan
]

DERIVED_SPREADS = [
    ("^IXIC_MINUS_GSPC", "^IXIC", "^GSPC"),
    ("^RUT_MINUS_GSPC",  "^RUT",  "^GSPC"),
]


def _fetch_one(symbol: str) -> Optional[pd.DataFrame]:
    """Fetch full-history daily bars for one symbol with one retry."""
    for attempt in (1, 2):
        try:
            df = yf.Ticker(symbol).history(
                period="max", interval="1d", auto_adjust=False
            )
            if df is None or df.empty:
                logger.warning(f"yfinance returned empty df for {symbol}")
                return None
            return df
        except Exception as e:
            if attempt == 1:
                logger.warning(f"yfinance fetch failed for {symbol} (attempt 1): {e} — retrying")
                time.sleep(5)
                continue
            logger.error(f"yfinance fetch failed for {symbol} (attempt 2): {e}")
            return None
    return None


def _insert_bars(conn, table: str, symbol_col: str, symbol: str, df: pd.DataFrame) -> int:
    """Insert a yfinance df into the given staging table. Returns rows inserted."""
    rows = 0
    has_div = "Dividends" in df.columns
    has_split = "Stock Splits" in df.columns
    has_adj = "Adj Close" in df.columns

    for idx, row in df.iterrows():
        ts = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)[:10]
        if table == "staging_yf_bars":
            conn.execute(
                f"""
                INSERT OR IGNORE INTO {table}
                    ({symbol_col}, ts, open, high, low, close, adj_close,
                     volume, dividends, splits)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol, ts,
                    float(row["Open"])      if pd.notna(row["Open"])      else None,
                    float(row["High"])      if pd.notna(row["High"])      else None,
                    float(row["Low"])       if pd.notna(row["Low"])       else None,
                    float(row["Close"])     if pd.notna(row["Close"])     else None,
                    float(row["Adj Close"]) if has_adj and pd.notna(row["Adj Close"]) else None,
                    float(row["Volume"])    if pd.notna(row["Volume"])    else None,
                    float(row["Dividends"]) if has_div and pd.notna(row["Dividends"]) else 0.0,
                    float(row["Stock Splits"]) if has_split and pd.notna(row["Stock Splits"]) else 0.0,
                ),
            )
        else:
            conn.execute(
                f"""
                INSERT OR IGNORE INTO {table}
                    ({symbol_col}, ts, open, high, low, close, adj_close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol, ts,
                    float(row["Open"])      if pd.notna(row["Open"])      else None,
                    float(row["High"])      if pd.notna(row["High"])      else None,
                    float(row["Low"])       if pd.notna(row["Low"])       else None,
                    float(row["Close"])     if pd.notna(row["Close"])     else None,
                    float(row["Adj Close"]) if has_adj and pd.notna(row["Adj Close"]) else None,
                    float(row["Volume"])    if pd.notna(row["Volume"])    else None,
                ),
            )
        rows += 1
    return rows


def run_yf_bars_etl(tickers: Optional[Iterable[str]] = None) -> int:
    """Fetch daily bars for tickers (default = 32 semi/Mag7) into staging_yf_bars."""
    tickers = list(tickers) if tickers is not None else sorted(SEMI_TICKERS)
    logger.info(f"yf-bars: {len(tickers)} tickers")
    total = 0
    with get_connection() as conn:
        for sym in tickers:
            df = _fetch_one(sym)
            if df is None:
                time.sleep(_RATE_DELAY)
                continue
            rows = _insert_bars(conn, "staging_yf_bars", "ticker", sym, df)
            conn.commit()
            total += rows
            logger.debug(f"yf-bars {sym}: {rows} rows")
            time.sleep(_RATE_DELAY)
    logger.info(f"yf-bars complete: {total:,} rows across {len(tickers)} tickers")
    return total


def _compute_derived_spreads(conn) -> int:
    """Compute derived spread rows (^IXIC_MINUS_GSPC, ^RUT_MINUS_GSPC)."""
    total = 0
    for out_symbol, left, right in DERIVED_SPREADS:
        result = conn.execute(
            """
            SELECT l.ts,
                   l.open  - r.open  AS open,
                   l.high  - r.high  AS high,
                   l.low   - r.low   AS low,
                   l.close - r.close AS close,
                   l.adj_close - r.adj_close AS adj_close
            FROM staging_yf_indices l
            JOIN staging_yf_indices r ON l.ts = r.ts
            WHERE l.symbol = ? AND r.symbol = ?
            ORDER BY l.ts
            """,
            (left, right),
        ).fetchall()
        for ts, o, h, lo, c, adj in result:
            conn.execute(
                """
                INSERT OR IGNORE INTO staging_yf_indices
                    (symbol, ts, open, high, low, close, adj_close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (out_symbol, ts, o, h, lo, c, adj),
            )
            total += 1
    conn.commit()
    return total


def _compute_index_stats(conn) -> int:
    """Rebuild staging_yf_index_stats from scratch using DuckDB window functions."""
    conn.execute("DELETE FROM staging_yf_index_stats")

    conn.execute(
        """
        INSERT INTO staging_yf_index_stats (
            symbol, ts,
            ret_1d, ret_21d, ret_252d,
            vol_21d, vol_63d, vol_252d,
            drawdown, max_drawdown_to_date,
            sharpe_252d, skew_252d, kurt_252d,
            mean_252d, sigma_252d,
            band_plus_1, band_minus_1,
            band_plus_2, band_minus_2,
            band_plus_3, band_minus_3,
            band_plus_4, band_minus_4
        )
        WITH base AS (
            SELECT
                symbol,
                ts,
                close,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts) AS rn,
                LN(close / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY ts), 0)) AS r,
                MAX(close) OVER (PARTITION BY symbol ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_peak
            FROM staging_yf_indices
            WHERE close IS NOT NULL AND close > 0
        ),
        rolled AS (
            SELECT
                symbol, ts, close, rn, r, running_peak,
                AVG(r) OVER w21  AS mean_21,
                AVG(r) OVER w63  AS mean_63,
                AVG(r) OVER w252 AS mean_252,
                STDDEV_SAMP(r) OVER w21  AS sd_21,
                STDDEV_SAMP(r) OVER w63  AS sd_63,
                STDDEV_SAMP(r) OVER w252 AS sd_252,
                SKEWNESS(r)   OVER w252 AS sk_252,
                KURTOSIS(r)   OVER w252 AS ku_252,
                SUM(r) OVER w21  AS sumr_21,
                SUM(r) OVER w252 AS sumr_252,
                COUNT(r) OVER w252 AS n_252
            FROM base
            WINDOW
                w21  AS (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 20  PRECEDING AND CURRENT ROW),
                w63  AS (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 62  PRECEDING AND CURRENT ROW),
                w252 AS (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 251 PRECEDING AND CURRENT ROW)
        )
        SELECT
            symbol,
            ts,
            r                                                   AS ret_1d,
            CASE WHEN rn >= 21  THEN sumr_21  END                AS ret_21d,
            CASE WHEN rn >= 252 THEN sumr_252 END                AS ret_252d,
            CASE WHEN rn >= 21  THEN sd_21  * SQRT(252) END      AS vol_21d,
            CASE WHEN rn >= 63  THEN sd_63  * SQRT(252) END      AS vol_63d,
            CASE WHEN n_252 >= 252 THEN sd_252 * SQRT(252) END   AS vol_252d,
            (close / NULLIF(running_peak, 0)) - 1.0              AS drawdown,
            MIN((close / NULLIF(running_peak, 0)) - 1.0)
                OVER (PARTITION BY symbol ORDER BY ts
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                                                 AS max_drawdown_to_date,
            CASE WHEN n_252 >= 252 AND sd_252 > 0
                 THEN (mean_252 * 252) / (sd_252 * SQRT(252))
            END                                                  AS sharpe_252d,
            CASE WHEN n_252 >= 252 THEN sk_252 END               AS skew_252d,
            CASE WHEN n_252 >= 252 THEN ku_252 END               AS kurt_252d,
            CASE WHEN n_252 >= 252 THEN mean_252 END             AS mean_252d,
            CASE WHEN n_252 >= 252 THEN sd_252   END             AS sigma_252d,
            CASE WHEN n_252 >= 252 THEN mean_252 + 1 * sd_252 END AS band_plus_1,
            CASE WHEN n_252 >= 252 THEN mean_252 - 1 * sd_252 END AS band_minus_1,
            CASE WHEN n_252 >= 252 THEN mean_252 + 2 * sd_252 END AS band_plus_2,
            CASE WHEN n_252 >= 252 THEN mean_252 - 2 * sd_252 END AS band_minus_2,
            CASE WHEN n_252 >= 252 THEN mean_252 + 3 * sd_252 END AS band_plus_3,
            CASE WHEN n_252 >= 252 THEN mean_252 - 3 * sd_252 END AS band_minus_3,
            CASE WHEN n_252 >= 252 THEN mean_252 + 4 * sd_252 END AS band_plus_4,
            CASE WHEN n_252 >= 252 THEN mean_252 - 4 * sd_252 END AS band_minus_4
        FROM rolled
        """
    )

    n = conn.execute("SELECT COUNT(*) FROM staging_yf_index_stats").fetchone()[0]
    conn.commit()
    return n


def run_yf_indices_etl(symbols: Optional[Iterable[str]] = None) -> int:
    """Fetch daily bars for INDICES, materialise derived spreads, recompute stats."""
    symbols = list(symbols) if symbols is not None else list(INDICES)
    logger.info(f"yf-indices: {len(symbols)} real symbols + {len(DERIVED_SPREADS)} derived")
    raw_total = 0
    with get_connection() as conn:
        for sym in symbols:
            df = _fetch_one(sym)
            if df is None:
                time.sleep(_RATE_DELAY)
                continue
            rows = _insert_bars(conn, "staging_yf_indices", "symbol", sym, df)
            conn.commit()
            raw_total += rows
            logger.debug(f"yf-indices {sym}: {rows} rows")
            time.sleep(_RATE_DELAY)

        derived = _compute_derived_spreads(conn)
        logger.info(f"yf-indices derived spreads: {derived} rows")

        stats = _compute_index_stats(conn)
        logger.info(f"yf-indices stats recomputed: {stats} rows")

    total = raw_total + derived + stats
    logger.info(f"yf-indices complete: {total:,} rows total")
    return total
