"""
etl/extract_polygon.py
ETL functions for polygon.io data.

Four jobs:
  run_polygon_bars_etl       – aggregate OHLCV bars (daily by default)
  run_polygon_snapshots_etl  – real-time / delayed stock snapshots
  run_polygon_options_etl    – options chain snapshots (greeks, IV, OI)
  run_polygon_reference_etl  – ticker reference / metadata
"""
import time
from datetime import datetime, timedelta, timezone, date
from typing import List, Optional

from loguru import logger
from polygon import RESTClient

from db.database import get_connection


# ── OHLCV bars ────────────────────────────────────────────────────────────────

def run_polygon_bars_etl(
    client: RESTClient,
    tickers: List[str],
    timespan: str = "day",
    lookback_days: int = 7,
) -> int:
    """Fetch aggregate bars and upsert into polygon_bars."""
    from_ = (date.today() - timedelta(days=lookback_days)).isoformat()
    to_   = date.today().isoformat()
    total = 0

    with get_connection() as conn:
        for ticker in tickers:
            try:
                aggs = client.get_aggs(
                    ticker, 1, timespan, from_, to_,
                    adjusted=True, limit=5000,
                )
                rows = 0
                for a in aggs:
                    ts = _ms_to_iso(getattr(a, "timestamp", None))
                    conn.execute("""
                        INSERT OR REPLACE INTO polygon_bars
                            (ticker, ts, timespan, open, high, low, close,
                             volume, vwap, transactions)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        ticker, ts, timespan,
                        getattr(a, "open",         None),
                        getattr(a, "high",         None),
                        getattr(a, "low",          None),
                        getattr(a, "close",        None),
                        getattr(a, "volume",       None),
                        getattr(a, "vwap",         None),
                        getattr(a, "transactions", None),
                    ))
                    rows += 1
                conn.commit()
                total += rows
                logger.debug(f"polygon bars {ticker}: {rows} bars ({timespan})")
            except Exception as e:
                logger.warning(f"polygon bars failed for {ticker}: {e}")

    logger.info(f"polygon bars ETL complete: {total} rows across {len(tickers)} tickers")
    return total


# ── Real-time / delayed snapshots ─────────────────────────────────────────────

def run_polygon_snapshots_etl(
    client: RESTClient,
    tickers: List[str],
) -> int:
    """Fetch stock snapshots and insert into polygon_snapshots."""
    total = 0
    ts    = _utcnow()

    # Batch all tickers in one API call
    try:
        snapshots = client.get_snapshot_all("stocks", ticker_symbols=tickers)
        with get_connection() as conn:
            for snap in snapshots:
                ticker     = getattr(snap, "ticker", None)
                last_quote = getattr(snap, "lastQuote", None) or getattr(snap, "last_quote", None)
                last_trade = getattr(snap, "lastTrade", None) or getattr(snap, "last_trade", None)
                day        = getattr(snap, "day",      None)
                prev_day   = getattr(snap, "prevDay",  None) or getattr(snap, "prev_day", None)

                bid        = _nested(last_quote, "p")       # bid price
                ask        = _nested(last_quote, "P")       # ask price (capital P)
                last       = _nested(last_trade, "p")       # last trade price
                prev_close = _nested(prev_day,   "c")
                day_volume = _nested(day,        "v")

                conn.execute("""
                    INSERT INTO polygon_snapshots
                        (ticker, ts, bid, ask, last, prev_close, day_volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (ticker, ts, bid, ask, last, prev_close, day_volume))
                total += 1

            conn.commit()
        logger.info(f"polygon snapshots ETL complete: {total} tickers")
    except Exception as e:
        logger.error(f"polygon snapshots ETL failed: {e}")

    return total


# ── Options chain snapshots ───────────────────────────────────────────────────

def run_polygon_options_etl(
    client: RESTClient,
    tickers: List[str],
    max_per_ticker: int = 500,
) -> int:
    """Fetch options chain snapshots and insert into polygon_option_snapshots."""
    total = 0
    ts    = _utcnow()

    with get_connection() as conn:
        for ticker in tickers:
            try:
                count = 0
                for opt in client.list_snapshot_options_chain(ticker):
                    if count >= max_per_ticker:
                        break

                    details = getattr(opt, "details", None)
                    greeks  = getattr(opt, "greeks",  None)
                    day     = getattr(opt, "day",     None)

                    expiry        = _nested(details, "expiration_date")
                    strike        = _nested(details, "strike_price")
                    contract_type = _nested(details, "contract_type")   # 'call' | 'put'

                    conn.execute("""
                        INSERT INTO polygon_option_snapshots
                            (underlying, expiry, strike, "right", ts,
                             day_open, day_close, day_volume,
                             open_interest, implied_vol,
                             delta, gamma, theta, vega)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        ticker, expiry, strike, contract_type, ts,
                        _nested(day, "open"),
                        _nested(day, "close"),
                        _nested(day, "volume"),
                        getattr(opt, "open_interest",    None),
                        getattr(opt, "implied_volatility", None),
                        _nested(greeks, "delta"),
                        _nested(greeks, "gamma"),
                        _nested(greeks, "theta"),
                        _nested(greeks, "vega"),
                    ))
                    count += 1

                conn.commit()
                total += count
                logger.debug(f"polygon options {ticker}: {count} contracts")
            except Exception as e:
                logger.warning(f"polygon options failed for {ticker}: {e}")

    logger.info(f"polygon options ETL complete: {total} contracts across {len(tickers)} tickers")
    return total


# ── Ticker reference / metadata ───────────────────────────────────────────────

def run_polygon_reference_etl(
    client: RESTClient,
    tickers: List[str],
) -> int:
    """Fetch ticker details and upsert into polygon_tickers."""
    total = 0

    with get_connection() as conn:
        for ticker in tickers:
            try:
                d = client.get_ticker_details(ticker)
                conn.execute("""
                    INSERT OR REPLACE INTO polygon_tickers
                        (ticker, name, market, primary_exchange, type,
                         active, currency, description, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ticker,
                    getattr(d, "name",             None),
                    getattr(d, "market",           None),
                    getattr(d, "primary_exchange", None),
                    getattr(d, "type",             None),
                    1 if getattr(d, "active", True) else 0,
                    getattr(d, "currency_name",    None),
                    getattr(d, "description",      None),
                    _utcnow(),
                ))
                total += 1
                time.sleep(0.1)   # be gentle on the API
            except Exception as e:
                logger.warning(f"polygon reference failed for {ticker}: {e}")

        conn.commit()
    logger.info(f"polygon reference ETL complete: {total} tickers")
    return total


# ── Helpers ───────────────────────────────────────────────────────────────────

def _nested(obj, attr: str):
    """Safely get obj.attr, returning None if obj is None or attr is missing."""
    if obj is None:
        return None
    return getattr(obj, attr, None)


def _ms_to_iso(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="seconds")


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
