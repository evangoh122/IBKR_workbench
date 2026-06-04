"""
etl/extract_polygon.py
ETL functions for polygon.io data.

Four jobs:
  run_polygon_bars_etl       – aggregate OHLCV bars (daily by default)
  run_polygon_snapshots_etl  – real-time / delayed stock snapshots
  run_polygon_options_etl    – options chain snapshots (greeks, IV, OI)
  run_polygon_reference_etl  – ticker reference / metadata
"""
import os
import time
from datetime import datetime, timedelta, timezone, date
from typing import List, Optional

from loguru import logger
from polygon import RESTClient

from db.database import get_connection


# ── OHLCV bars ────────────────────────────────────────────────────────────────

def run_polygon_bars_etl(
    client: RESTClient,
    tickers: List[dict],
    timespan: str = "day",
    lookback_days: int = 7,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> int:
    """Fetch aggregate bars and upsert into polygon_bars."""
    from_ = from_date or (date.today() - timedelta(days=lookback_days)).isoformat()
    to_   = to_date   or date.today().isoformat()
    total = 0

    with get_connection() as conn:
        for t_def in tickers:
            symbol = t_def.get("symbol")
            try:
                aggs = client.get_aggs(
                    _polygon_ticker(t_def), 1, timespan, from_, to_,
                    adjusted=True, limit=50000,
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
                        symbol, ts, timespan,
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
                logger.debug(f"polygon bars {symbol}: {rows} bars ({timespan})")
            except Exception as e:
                logger.warning(f"polygon bars failed for {symbol}: {e}")
            finally:
                # Always sleep — skipping on failures causes 429 cascades
                time.sleep(_RATE_DELAY)

    logger.info(f"polygon bars ETL complete: {total} rows across {len(tickers)} tickers")
    return total


# ── Real-time / delayed snapshots ─────────────────────────────────────────────

def run_polygon_snapshots_etl(
    client: RESTClient,
    tickers: List[dict],
) -> int:
    """Fetch snapshots and insert into polygon_snapshots."""
    total = 0
    ts    = _utcnow()

    # Map IBKR secType to Polygon market type
    market_map = {
        "STK": "stocks",
        "CASH": "forex",
        "IND": "indices"
    }

    # Group polygon formatted tickers by market type
    groups = {}
    for t_def in tickers:
        sec_type = t_def.get("secType", "STK")
        market = market_map.get(sec_type)
        if market:
            groups.setdefault(market, []).append(_polygon_ticker(t_def))
        else:
            logger.debug(f"Skipping unsupported snapshot secType: {sec_type} for {t_def.get('symbol')}")

    with get_connection() as conn:
        for market, poly_tickers in groups.items():
            try:
                time.sleep(_RATE_DELAY)
                snapshots = client.get_snapshot_all(market, ticker_symbols=",".join(poly_tickers))
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
                logger.info(f"polygon snapshots ({market}) complete: {len(poly_tickers)} tickers requested")
            except Exception as e:
                logger.error(f"polygon snapshots ({market}) failed: {e}")

    return total


# ── Options chain snapshots ───────────────────────────────────────────────────

def run_polygon_options_etl(
    client: RESTClient,
    tickers: List[dict],
    max_per_ticker: int = 500,
) -> int:
    """Fetch options chain snapshots and insert into polygon_option_snapshots."""
    total = 0
    ts    = _utcnow()

    with get_connection() as conn:
        for t_def in tickers:
            symbol = t_def.get("symbol")
            if t_def.get("secType") in ("CASH",):
                continue  # Forex options typically not supported via this endpoint
            
            try:
                count = 0
                time.sleep(_RATE_DELAY)
                for opt in client.list_snapshot_options_chain(_polygon_ticker(t_def)):
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
                        symbol, expiry, strike, contract_type, ts,
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
                logger.debug(f"polygon options {symbol}: {count} contracts")
            except Exception as e:
                if "NOT_AUTHORIZED" in str(e) or "not entitled" in str(e).lower():
                    logger.error(
                        "Polygon options require a paid plan — aborting job. "
                        "Upgrade at https://polygon.io/dashboard/api-keys"
                    )
                    return 0
                logger.warning(f"polygon options failed for {symbol}: {e}")

    logger.info(f"polygon options ETL complete: {total} contracts across {len(tickers)} tickers")
    return total


# ── Ticker reference / metadata ───────────────────────────────────────────────

def run_polygon_reference_etl(
    client: RESTClient,
    tickers: List[dict],
) -> int:
    """Fetch ticker details and upsert into polygon_tickers."""
    total = 0

    # Reference data (descriptions, etc.) is most useful and reliable for stocks.
    # Futures and Indices often cause 404s or 429s on the free tier.
    stk_only = [t for t in tickers if t.get("secType", "STK") == "STK"]

    with get_connection() as conn:
        for t_def in stk_only:
            symbol = t_def.get("symbol")
            poly_ticker = _polygon_ticker(t_def)
            try:
                d = client.get_ticker_details(poly_ticker)
                conn.execute("""
                    INSERT OR REPLACE INTO polygon_tickers
                        (ticker, name, market, primary_exchange, type,
                         active, currency, description, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol,
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
                logger.debug(f"polygon reference {symbol}: updated")
            except Exception as e:
                if "429" in str(e):
                    logger.warning(f"polygon reference 429 for {symbol} — sleeping 60s")
                    time.sleep(60)
                else:
                    logger.warning(f"polygon reference failed for {symbol}: {e}")
            finally:
                # Always sleep — skipping on failures causes 429 cascades
                time.sleep(_RATE_DELAY)

        conn.commit()
    logger.info(f"polygon reference ETL complete: {total} tickers")
    return total


# ── Historical options OHLCV bars ────────────────────────────────────────────

def run_polygon_option_bars_etl(
    client: RESTClient,
    tickers: List[dict],
    timespan: str = "day",
    lookback_days: int = 9500,
    max_contracts: int = 250,
) -> int:
    """
    Fetch historical OHLCV + VWAP bars for options contracts.

    For each underlying:
      1. List all available option contracts via polygon (active + expired)
      2. Fetch daily bars for each contract
      3. Upsert into polygon_option_bars

    Scale note: each contract = 1 API call.  Limit tickers with
    POLYGON_OPTION_BARS_TICKERS in .env (comma-separated, default = 12 liquid names).
    """
    from_ = (date.today() - timedelta(days=lookback_days)).isoformat()
    to_   = date.today().isoformat()
    total = 0

    # Only equities — options on forex/futures handled separately
    stk_tickers = [t for t in tickers if t.get("secType", "STK") == "STK"]

    # Respect per-ticker override list
    override = os.getenv("POLYGON_OPTION_BARS_TICKERS", "")
    if override:
        want = {s.strip().upper() for s in override.split(",")}
        stk_tickers = [t for t in stk_tickers if t.get("symbol", "").upper() in want]

    logger.info(
        f"polygon option bars: fetching for {len(stk_tickers)} underlyings, "
        f"up to {max_contracts} contracts each"
    )

    with get_connection() as conn:
        for t_def in stk_tickers:
            underlying  = t_def.get("symbol", "")
            poly_ticker = _polygon_ticker(t_def)

            # ── Step 1: list contracts ────────────────────────────────────
            contracts = []
            try:
                time.sleep(_RATE_DELAY)
                for contract in client.list_options_contracts(
                    underlying_ticker=poly_ticker,
                    expired=True,           # include expired for full history
                    limit=max_contracts,
                ):
                    contracts.append(contract)
                    if len(contracts) >= max_contracts:
                        break
            except Exception as e:
                if "NOT_AUTHORIZED" in str(e) or "not entitled" in str(e).lower():
                    logger.error(
                        "Polygon option bars require a paid plan — aborting job. "
                        "Upgrade at https://polygon.io/dashboard/api-keys"
                    )
                    return 0
                logger.warning(f"Couldn't list contracts for {underlying}: {e}")
                continue

            if not contracts:
                logger.debug(f"{underlying}: no contracts returned")
                continue

            logger.info(f"{underlying}: {len(contracts)} contracts — fetching bars…")

            # ── Step 2: bars per contract ─────────────────────────────────
            ticker_rows = 0
            for contract in contracts:
                opt_ticker = getattr(contract, "ticker", None)
                if not opt_ticker:
                    continue

                expiry = getattr(contract, "expiration_date", None)
                strike = getattr(contract, "strike_price",    None)
                right  = getattr(contract, "contract_type",   None)  # 'call' | 'put'

                time.sleep(_RATE_DELAY)
                try:
                    aggs = client.get_aggs(
                        opt_ticker, 1, timespan, from_, to_,
                        adjusted=False,   # options are not split-adjusted
                        limit=50000,
                    )
                    for a in aggs:
                        ts = _ms_to_iso(getattr(a, "timestamp", None))
                        conn.execute("""
                            INSERT OR REPLACE INTO polygon_option_bars
                                (option_ticker, underlying, expiry, strike, "right",
                                 ts, timespan, open, high, low, close,
                                 volume, vwap, transactions)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            opt_ticker, underlying, expiry, strike, right,
                            ts, timespan,
                            getattr(a, "open",         None),
                            getattr(a, "high",         None),
                            getattr(a, "low",          None),
                            getattr(a, "close",        None),
                            getattr(a, "volume",       None),
                            getattr(a, "vwap",         None),
                            getattr(a, "transactions", None),
                        ))
                        ticker_rows += 1

                except Exception as e:
                    logger.debug(f"  bars failed for {opt_ticker}: {e}")

            conn.commit()
            total += ticker_rows
            logger.info(f"{underlying}: {ticker_rows} option bar rows written")

    logger.info(f"polygon option bars ETL complete: {total} rows")
    return total


# ── Helpers ───────────────────────────────────────────────────────────────────

# Free tier: 5 req/min → 13s between calls.  Paid tiers: set POLYGON_RATE_DELAY=0.1
_RATE_DELAY = float(os.getenv("POLYGON_RATE_DELAY", "13"))


def _polygon_ticker(t_def: dict) -> str:
    """
    Convert an IBKR ticker dict to the polygon.io ticker format.

    STK:  AAPL        → AAPL   (BRK B → BRK.B)
    CASH: EUR.USD     → C:EURUSD
    IND:  SPX         → I:SPX
    FUT:  ES          → F:ES
    """
    if not isinstance(t_def, dict):
        return str(t_def).strip().replace(" ", ".")

    sec_type = t_def.get("secType", "STK")
    symbol   = t_def.get("symbol", "").strip()

    if sec_type == "CASH":
        # EUR.USD → EURUSD  (strip the dot separator polygon doesn't use)
        return "C:" + symbol.replace(".", "")

    if sec_type == "IND":
        return "I:" + symbol

    if sec_type == "FUT":
        return "F:" + symbol

    # STK default — normalise IBKR space to polygon dot (e.g. BRK B → BRK.B)
    return symbol.replace(" ", ".")


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
