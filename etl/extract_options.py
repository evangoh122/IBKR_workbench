"""
etl/extract_options.py
Two-phase options ETL:
  1. Discover / refresh option chain metadata (expiries + strikes)
  2. Pull live quotes + Greeks for selected contracts
"""
import os
import threading
from datetime import datetime, timezone
from typing import List

from loguru import logger

from db.database import get_connection
from etl.ibkr_client import IBKRClient
from config.tickers import get_expiry_cycles


# How many nearest expiries to actually quote (chain discovery returns all)
DEFAULT_EXPIRY_CYCLES = int(os.getenv("OPTIONS_EXPIRY_CYCLES", 2))


# ── Phase 1: Chain Discovery ──────────────────────────────────────────────────

def refresh_option_chains(client: IBKRClient, tickers: List[str]) -> int:
    """
    Fetch full option chains (all expiries/strikes) for each ticker.
    Upserts into option_chains metadata table.
    Returns total contracts stored.
    """
    total = 0
    conn  = get_connection()

    for ticker in tickers:
        logger.info(f"Fetching option chain for {ticker}…")
        chain = client.request_option_chain(ticker, timeout=30)

        if not chain:
            logger.warning(f"No chain data returned for {ticker}")
            continue

        # Prefer SMART exchange; fall back to first available
        smart = [(exp, strike, right)
                 for (exch, exp, strike, right) in chain
                 if exch == "SMART"]
        rows  = smart or [(exp, strike, right)
                          for (_, exp, strike, right) in chain]

        conn.executemany("""
            INSERT OR REPLACE INTO option_chains (ticker, expiry, strike, right)
            VALUES (?, ?, ?, ?)
        """, [(ticker, exp, strike, right) for (exp, strike, right) in rows])
        conn.commit()

        total += len(rows)
        logger.info(f"{ticker}: stored {len(rows)} chain entries")

    conn.close()
    return total


# ── Phase 2: Quote Selected Contracts ────────────────────────────────────────

def run_option_etl(client: IBKRClient, tickers: List[str],
                   expiry_cycles: int = DEFAULT_EXPIRY_CYCLES) -> int:
    """
    Pull live option quotes for the nearest N expiry cycles per ticker.
    Returns number of rows written.
    """
    conn = get_connection()

    # Gather contracts to quote
    contracts_to_quote = []   # (ticker, expiry, strike, right)

    for ticker in tickers:
        rows = conn.execute("""
            SELECT DISTINCT expiry FROM option_chains
            WHERE ticker = ?
            ORDER BY expiry ASC
        """, (ticker,)).fetchall()

        expiries = [r["expiry"] for r in rows][:get_expiry_cycles(ticker, expiry_cycles)]
        if not expiries:
            logger.warning(f"No chain data in DB for {ticker} — run chain refresh first")
            continue

        for expiry in expiries:
            strikes = conn.execute("""
                SELECT strike, right FROM option_chains
                WHERE ticker=? AND expiry=?
                ORDER BY strike
            """, (ticker, expiry)).fetchall()
            for s in strikes:
                contracts_to_quote.append((ticker, expiry, s["strike"], s["right"]))

    conn.close()

    if not contracts_to_quote:
        logger.warning("No option contracts to quote")
        return 0

    logger.info(f"Quoting {len(contracts_to_quote)} option contracts…")

    # Request snapshots concurrently
    results   = {}
    meta      = {}   # req_id -> (ticker, expiry, strike, right)
    lock      = threading.Lock()
    done_evts = {}

    def on_done(req_id: int, snap: dict):
        with lock:
            results[req_id] = snap
            ev = done_evts.get(req_id)
            if ev:
                ev.set()

    # Throttle: IBKR allows ~50 concurrent snapshot requests
    BATCH = 50
    all_rows = []

    for i in range(0, len(contracts_to_quote), BATCH):
        batch = contracts_to_quote[i:i + BATCH]
        batch_reqs = {}

        for (ticker, expiry, strike, right) in batch:
            contract = client.make_option_contract(ticker, expiry, strike, right)
            ev       = threading.Event()
            req_id   = client.request_snapshot(contract, on_done)
            with lock:
                done_evts[req_id] = ev
                if req_id in results:   # callback already fired before we registered
                    ev.set()
            batch_reqs[req_id] = (ticker, expiry, strike, right)
            meta[req_id] = (ticker, expiry, strike, right)

        # Wait for batch
        for req_id in batch_reqs:
            done_evts[req_id].wait(timeout=15)

        # Collect rows from this batch
        for req_id, (ticker, expiry, strike, right) in batch_reqs.items():
            snap = results.get(req_id, {})
            if not snap:
                continue
            all_rows.append({
                "ticker":        ticker,
                "expiry":        expiry,
                "strike":        strike,
                "right":         right,
                "ts":            snap.get("ts", _utcnow()),
                "bid":           snap.get("bid"),
                "ask":           snap.get("ask"),
                "last":          snap.get("last"),
                "volume":        snap.get("volume"),
                "open_interest": snap.get("open_interest"),
                "implied_vol":   snap.get("implied_vol"),
                "delta":         snap.get("delta"),
                "gamma":         snap.get("gamma"),
                "theta":         snap.get("theta"),
                "vega":          snap.get("vega"),
            })

        logger.debug(f"Batch {i//BATCH + 1}: collected {len(all_rows)} rows so far")

    # Write all rows
    if not all_rows:
        logger.warning("No option quote data received")
        return 0

    conn = get_connection()
    conn.executemany("""
        INSERT INTO option_quotes
            (ticker, expiry, strike, right, ts, bid, ask, last,
             volume, open_interest, implied_vol, delta, gamma, theta, vega)
        VALUES
            (:ticker, :expiry, :strike, :right, :ts, :bid, :ask, :last,
             :volume, :open_interest, :implied_vol, :delta, :gamma, :theta, :vega)
    """, all_rows)
    conn.commit()
    conn.close()

    logger.info(f"Wrote {len(all_rows)} option quote rows")
    return len(all_rows)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
