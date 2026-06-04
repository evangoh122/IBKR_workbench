"""
etl/extract_polygon_ticks.py
Fetches individual trade ticks from Polygon /v3/trades and stores
them in the polygon_trades table.

Scale warning:
  A single liquid ticker (e.g. SPY) generates ~5-10M trades/day.
  5 years = ~9 billion rows for SPY alone.
  Limit POLYGON_TICK_TICKERS to a small watchlist and expect multi-hour
  runs even on a paid plan.

Run:
    python main.py --job polygon-ticks
"""
import time
from datetime import datetime, timezone
from typing import List

from loguru import logger
from polygon import RESTClient

from db.database import get_connection

_RATE_DELAY = None   # resolved at call time from extract_polygon._RATE_DELAY


def _delay() -> float:
    from etl.extract_polygon import _RATE_DELAY as d
    return d


def run_polygon_ticks_etl(
    client: RESTClient,
    tickers: List[dict],
    from_date: str,
    to_date: str,
    max_per_ticker: int = 10_000_000,
) -> int:
    """
    Fetch trade ticks for each ticker between from_date and to_date.

    Parameters
    ----------
    max_per_ticker : hard cap per ticker to prevent runaway storage.
                     Default 10M rows ≈ ~1-2 days of SPY trades.
    """
    total = 0
    stk_tickers = [t for t in tickers if t.get("secType", "STK") == "STK"]

    logger.info(
        f"polygon-ticks: {len(stk_tickers)} tickers, "
        f"{from_date} → {to_date}, cap {max_per_ticker:,}/ticker"
    )

    with get_connection() as conn:
        for t_def in stk_tickers:
            symbol     = t_def.get("symbol", "")
            poly_ticker = symbol.replace(" ", ".")   # BRK B → BRK.B

            count = 0
            try:
                for trade in client.list_trades(
                    poly_ticker,
                    params={
                        "timestamp.gte": f"{from_date}T00:00:00.000000000Z",
                        "timestamp.lte": f"{to_date}T23:59:59.999999999Z",
                    },
                    limit=50000,
                ):
                    if count >= max_per_ticker:
                        logger.warning(
                            f"{symbol}: hit max_per_ticker={max_per_ticker:,} — stopping early"
                        )
                        break

                    sip_ns = getattr(trade, "sip_timestamp", None)
                    ts = (
                        datetime.fromtimestamp(sip_ns / 1e9, tz=timezone.utc)
                        .isoformat(timespec="microseconds")
                        if sip_ns is not None else None
                    )

                    conditions = getattr(trade, "conditions", None) or []
                    conn.execute("""
                        INSERT OR IGNORE INTO polygon_trades
                            (ticker, ts, price, size, conditions, exchange, tape)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        symbol,
                        ts,
                        getattr(trade, "price",    None),
                        getattr(trade, "size",     None),
                        ",".join(str(c) for c in conditions) if conditions else None,
                        getattr(trade, "exchange", None),
                        getattr(trade, "tape",     None),
                    ))
                    count += 1

                    # Commit + log every 500k rows to avoid huge transactions
                    if count % 500_000 == 0:
                        conn.commit()
                        logger.info(f"{symbol}: {count:,} ticks stored so far…")

                conn.commit()
                total += count
                logger.info(f"{symbol}: {count:,} ticks stored")

            except Exception as e:
                if "NOT_AUTHORIZED" in str(e) or "not entitled" in str(e).lower():
                    logger.error(
                        "Polygon tick data requires a paid plan — aborting. "
                        "Upgrade at https://polygon.io/dashboard/api-keys"
                    )
                    return 0
                logger.warning(f"polygon ticks failed for {symbol}: {e}")
            finally:
                time.sleep(_delay())

    logger.info(f"polygon-ticks ETL complete: {total:,} trades across {len(stk_tickers)} tickers")
    return total
