"""
etl/extract_stocks.py
Pulls stock quotes (bid/ask/last/volume/OHLC) for configured tickers
and writes them to the stock_quotes table.
"""
import time
import threading
from datetime import datetime, timezone
from typing import List

from loguru import logger

from db.database import get_connection
from etl.ibkr_client import IBKRClient


def run_stock_etl(client: IBKRClient, tickers: List[str]) -> int:
    """
    Request snapshots for all tickers, collect results, write to DB.
    Returns number of rows written.
    """
    results   = {}
    lock      = threading.Lock()
    done_evts = {}

    def on_done(req_id: int, snap: dict):
        with lock:
            results[req_id] = snap
            ev = done_evts.get(req_id)
            if ev:
                ev.set()

    # Fire all snapshot requests
    req_map = {}   # req_id -> ticker
    for ticker in tickers:
        contract = client.make_stock_contract(ticker)
        ev = threading.Event()
        req_id = client.request_snapshot(contract, on_done)
        with lock:
            done_evts[req_id] = ev
            if req_id in results:   # callback already fired before we registered
                ev.set()
        req_map[req_id] = ticker
        logger.debug(f"Requested snapshot req={req_id} ticker={ticker}")

    # Wait for all (max 15 s per ticker)
    timeout = 15
    for req_id, ev in done_evts.items():
        ev.wait(timeout=timeout)

    # Write to DB
    rows = []
    for req_id, snap in results.items():
        if not snap:
            logger.warning(f"Empty snapshot for {req_map.get(req_id)}")
            continue
        rows.append({
            "ticker":  req_map[req_id],
            "ts":      snap.get("ts", _utcnow()),
            "bid":     snap.get("bid"),
            "ask":     snap.get("ask"),
            "last":    snap.get("last"),
            "close":   snap.get("close"),
            "volume":  snap.get("volume"),
            "open":    snap.get("open"),
            "high":    snap.get("high"),
            "low":     snap.get("low"),
            "vwap":    snap.get("vwap"),
        })

    if not rows:
        logger.warning("No stock data received from TWS")
        return 0

    with get_connection() as conn:
        conn.executemany("""
            INSERT INTO stock_quotes
                (ticker, ts, bid, ask, last, close, volume, open, high, low, vwap)
            VALUES
                (:ticker, :ts, :bid, :ask, :last, :close, :volume, :open, :high, :low, :vwap)
        """, rows)
        conn.commit()

    logger.info(f"Wrote {len(rows)} stock quote rows")
    return len(rows)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
