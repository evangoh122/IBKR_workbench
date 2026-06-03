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
TICK_VWAP      = 37
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

        # Contract detail results (used to resolve conId before chain requests)
        self._detail_results: Dict[int, list] = {}
        self._detail_done:    Dict[int, threading.Event] = {}

    # ── Connection helpers ────────────────────────────────────────────────────

    def next_req_id(self) -> int:
        with self._lock:
            self._req_id += 1
            return self._req_id

    def connect_and_run(self):
        """Connect to TWS and start the reader thread."""
        # Capture before calling connect() — ibapi resets self.host/self.port
        # to None on a failed connection, which garbles the error message.
        host, port = self.host, self.port
        self.connect(host, port, self.client_id)
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()
        # Wait up to 10 s for nextValidId callback (signals ready)
        if not self._connected.wait(timeout=10):
            raise ConnectionError(
                f"Could not connect to TWS at {host}:{port}. "
                "Ensure TWS/Gateway is running and API connections are enabled."
            )
        logger.info(f"Connected to TWS @ {host}:{port} (client {self.client_id})")

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

    def tickGeneric(self, reqId, tickType, value):
        if tickType == TICK_VWAP:
            with self._lock:
                self._snapshots.setdefault(reqId, {})["ts"] = _utcnow()
                self._snapshots[reqId]["vwap"] = value

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
                snap["und_price"]   = undPrice   if undPrice   is not None else None
                snap["pv_dividend"] = pvDividend if pvDividend is not None else None

    def tickSnapshotEnd(self, reqId):
        """Called when a snapshot request is complete."""
        cb = self._on_snapshot.pop(reqId, None)
        if cb:
            snap = self._snapshots.pop(reqId, {})
            self._req_contracts.pop(reqId, None)
            cb(reqId, snap)

    # Contract detail callbacks (used to resolve conId)
    def contractDetails(self, reqId, contractDetails):
        self._detail_results.setdefault(reqId, []).append(contractDetails)

    def contractDetailsEnd(self, reqId):
        ev = self._detail_done.get(reqId)
        if ev:
            ev.set()

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

    def make_index_contract(self, ticker: str) -> Contract:
        c = Contract()
        c.symbol   = ticker
        c.secType  = "IND"
        c.currency = "USD"
        # Major indices like SPX and VIX are on CBOE
        c.exchange = "CBOE"
        return c

    def make_forex_contract(self, ticker: str) -> Contract:
        # e.g., ticker="EUR.USD" or just "EUR" with currency="USD"
        parts = ticker.split('.')
        c = Contract()
        c.symbol   = parts[0]
        c.secType  = "CASH"
        c.currency = parts[1] if len(parts) > 1 else "USD"
        c.exchange = "IDEALPRO"
        return c

    def make_future_contract(self, ticker: str, expiry: str) -> Contract:
        c = Contract()
        c.symbol   = ticker
        c.secType  = "FUT"
        c.currency = "USD"
        c.exchange = "CME"
        c.lastTradeDateOrContractMonth = expiry
        return c

    def make_contract(self, symbol: str, secType: str = "STK", exchange: str = "SMART", currency: str = "USD", expiry: str = "") -> Contract:
        """Universal contract builder."""
        # Special handling for Forex strings (e.g. "EUR.USD")
        if secType == "CASH" and "." in symbol:
            parts = symbol.split(".")
            symbol = parts[0]
            currency = parts[1]

        c = Contract()
        c.symbol = symbol
        c.secType = secType
        c.exchange = exchange
        c.currency = currency
        if expiry:
            c.lastTradeDateOrContractMonth = expiry
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
        # snapshot=True requires an empty genericTickList — IBKR rejects
        # generic ticks (e.g. "233" for VWAP) on snapshot requests (error 321)
        self.reqMktData(req_id, contract, "", True, False, [])
        return req_id

    def resolve_con_id(self, ticker: str, timeout: int = 10, **kwargs) -> int:
        """Resolve a ticker's conId via reqContractDetails. Returns 0 on failure."""
        contract = self.make_contract(symbol=ticker, **kwargs)
        req_id = self.next_req_id()
        done_event = threading.Event()
        self._detail_results[req_id] = []
        self._detail_done[req_id]    = done_event

        self.reqContractDetails(req_id, contract)
        done_event.wait(timeout=timeout)

        results = self._detail_results.pop(req_id, [])
        self._detail_done.pop(req_id, None)
        if results:
            return results[0].contract.conId
        logger.warning(f"Could not resolve conId for {ticker}")
        return 0

    def request_option_chain(self, ticker: str,
                              timeout: int = 30, **kwargs) -> list:
        """
        Synchronously fetch all option chain parameters for a ticker.
        Resolves the underlying conId first — required by some TWS versions.
        Returns list of (exchange, expiry, strike, right) tuples.
        """
        con_id = self.resolve_con_id(ticker, **kwargs)

        req_id = self.next_req_id()
        done_event = threading.Event()
        self._chain_results[req_id] = []
        self._chain_done[req_id]    = done_event

        self.reqSecDefOptParams(req_id, ticker, "", kwargs.get('secType', 'STK'), con_id)
        done_event.wait(timeout=timeout)

        results = self._chain_results.pop(req_id, [])
        self._chain_done.pop(req_id, None)
        return results


# ── Helpers ───────────────────────────────────────────────────────────────────

def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
