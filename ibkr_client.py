"""
etl/ibkr_client.py
Thread-safe IBKR TWS API wrapper.
Handles connection, market data requests, and option chain discovery.
"""
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Dict, Optional

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.ticktype import TickTypeEnum
from loguru import logger


# ── Tick-type mappings we care about ──────────────────────────────────────────
TICK_BID       = 1
TICK_ASK       = 2
TICK_LAST      = 4
TICK_VOLUME    = 8
TICK_CLOSE     = 9
TICK_OPEN      = 14
TICK_HIGH      = 6
TICK_LOW       = 7
TICK_OI        = 22   # open interest (options)
TICK_IV        = 24   # implied volatility (options)
TICK_DELTA     = 25
TICK_GAMMA     = 26
TICK_VEGA      = 27
TICK_THETA     = 28


class IBKRClient(EWrapper, EClient):
    """
    Combines EWrapper (callbacks) + EClient (requests).
    All callbacks store data into internal dicts; callers
    can register listeners or poll the snapshot dicts.
    """

    def __init__(self, host: str, port: int, client_id: int):
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)

        self.host      = host
        self.port      = port
        self.client_id = client_id

        self._lock        = threading.Lock()
        self._req_id      = 0          # auto-increment request IDs
        self._connected   = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # ── Live snapshots ────────────────────────────────────────────
        # req_id -> { field: value }
        self._snapshots: Dict[int, dict] = {}

        # req_id -> Contract (so we know what each req maps to)
        self._req_contracts: Dict[int, Contract] = {}

        # Callbacks registered by the ETL layer
        # req_id -> callable(req_id, snapshot_dict)
        self._on_snapshot: Dict[int, Callable] = {}

        # Option chain results
        # req_id -> list of (exchange, expiry, strike, right)
        self._chain_results: Dict[int, list] = {}
        self._chain_done:    Dict[int, threading.Event] = {}

    # ── Connection helpers ────────────────────────────────────────────────────

    def next_req_id(self) -> int:
        with self._lock:
            self._req_id += 1
            return self._req_id

    def connect_and_run(self):
        """Connect to TWS and start the reader thread."""
        self.connect(self.host, self.port, self.client_id)
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()
        # Wait up to 10 s for nextValidId callback (signals ready)
        if not self._connected.wait(timeout=10):
            raise ConnectionError(
                f"Could not connect to TWS at {self.host}:{self.port}. "
                "Ensure TWS/Gateway is running and API connections are enabled."
            )
        logger.info(f"Connected to TWS @ {self.host}:{self.port} (client {self.client_id})")

    def disconnect_and_stop(self):
        self.disconnect()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Disconnected from TWS")

    # ── EWrapper callbacks ────────────────────────────────────────────────────

    def nextValidId(self, orderId: int):
        self._req_id = orderId
        self._connected.set()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        # 2104/2106/2158 are informational (farm connected), not real errors
        if errorCode in (2104, 2106, 2158, 2119):
            logger.debug(f"TWS info [{errorCode}]: {errorString}")
        else:
            logger.warning(f"TWS error req={reqId} code={errorCode}: {errorString}")

    def tickPrice(self, reqId, tickType, price, attrib):
        mapping = {
            TICK_BID:   "bid",
            TICK_ASK:   "ask",
            TICK_LAST:  "last",
            TICK_CLOSE: "close",
            TICK_OPEN:  "open",
            TICK_HIGH:  "high",
            TICK_LOW:   "low",
        }
        field = mapping.get(tickType)
        if field and price > 0:
            with self._lock:
                self._snapshots.setdefault(reqId, {})["ts"] = _utcnow()
                self._snapshots[reqId][field] = price

    def tickSize(self, reqId, tickType, size):
        mapping = {
            TICK_VOLUME: "volume",
            TICK_OI:     "open_interest",
        }
        field = mapping.get(tickType)
        if field:
            with self._lock:
                self._snapshots.setdefault(reqId, {})["ts"] = _utcnow()
                self._snapshots[reqId][field] = size

    def tickOptionComputation(self, reqId, tickType, tickAttrib,
                               impliedVol, delta, optPrice, pvDividend,
                               gamma, vega, theta, undPrice):
        if tickType in (10, 11, 12, 13):   # bid/ask/last/model greeks
            with self._lock:
                snap = self._snapshots.setdefault(reqId, {})
                snap["ts"]          = _utcnow()
                snap["implied_vol"] = impliedVol if impliedVol != -1 else None
                snap["delta"]       = delta      if delta      != -2 else None
                snap["gamma"]       = gamma      if gamma      != -2 else None
                snap["vega"]        = vega       if vega       != -2 else None
                snap["theta"]       = theta      if theta      != -2 else None

    def tickSnapshotEnd(self, reqId):
        """Called when a snapshot request is complete."""
        cb = self._on_snapshot.get(reqId)
        if cb:
            snap = self._snapshots.get(reqId, {})
            cb(reqId, snap)

    # Option chain callbacks
    def securityDefinitionOptionParameter(
        self, reqId, exchange, underlyingConId,
        tradingClass, multiplier, expirations, strikes
    ):
        with self._lock:
            lst = self._chain_results.setdefault(reqId, [])
            for exp in expirations:
                for strike in strikes:
                    for right in ("C", "P"):
                        lst.append((exchange, exp, float(strike), right))

    def securityDefinitionOptionParameterEnd(self, reqId):
        ev = self._chain_done.get(reqId)
        if ev:
            ev.set()

    # ── Public API used by ETL layer ──────────────────────────────────────────

    def make_stock_contract(self, ticker: str) -> Contract:
        c = Contract()
        c.symbol   = ticker
        c.secType  = "STK"
        c.currency = "USD"
        c.exchange = "SMART"
        return c

    def make_option_contract(self, ticker: str, expiry: str,
                              strike: float, right: str) -> Contract:
        c = Contract()
        c.symbol      = ticker
        c.secType     = "OPT"
        c.currency    = "USD"
        c.exchange    = "SMART"
        c.lastTradeDateOrContractMonth = expiry
        c.strike      = strike
        c.right       = right
        c.multiplier  = "100"
        return c

    def request_snapshot(self, contract: Contract,
                          on_done: Callable[[int, dict], None]) -> int:
        """
        Request a one-shot market-data snapshot.
        on_done(req_id, snapshot_dict) is called when data arrives.
        Returns the req_id.
        """
        req_id = self.next_req_id()
        self._req_contracts[req_id] = contract
        self._on_snapshot[req_id]   = on_done
        self._snapshots[req_id]     = {}
        # snapshot=True → reqMktData with snapshot flag
        self.reqMktData(req_id, contract, "", True, False, [])
        return req_id

    def request_option_chain(self, ticker: str,
                              timeout: int = 30) -> list:
        """
        Synchronously fetch all option chain parameters for a ticker.
        Returns list of (exchange, expiry, strike, right) tuples.
        """
        req_id = self.next_req_id()
        done_event = threading.Event()
        self._chain_results[req_id] = []
        self._chain_done[req_id]    = done_event

        self.reqSecDefOptParams(req_id, ticker, "", "STK", 0)
        done_event.wait(timeout=timeout)

        results = self._chain_results.pop(req_id, [])
        self._chain_done.pop(req_id, None)
        return results


# ── Helpers ───────────────────────────────────────────────────────────────────

def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
