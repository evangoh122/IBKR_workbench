"""
tests/test_extract_options.py
Tests for option chain refresh and option quote extraction.
"""
import threading
import pytest
from unittest.mock import MagicMock
from db.database import get_connection
from etl.extract_options import refresh_option_chains, run_option_etl


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_chain_client(chains: dict):
    """chains = {ticker: [(exchange, expiry, strike, right), ...]}"""
    client = MagicMock()
    client.request_option_chain.side_effect = lambda ticker, **kw: chains.get(ticker, [])
    return client


def _make_quote_client(snapshots: dict):
    """snapshots keyed by (ticker, expiry, strike, right)."""
    req_counter = [0]

    def fake_snapshot(contract, on_done):
        req_id = req_counter[0]
        req_counter[0] += 1
        key = (contract.symbol, contract.lastTradeDateOrContractMonth,
               contract.strike, contract.right)
        snap = snapshots.get(key, {})
        t = threading.Thread(target=on_done, args=(req_id, snap))
        t.start()
        return req_id

    client = MagicMock()
    client.make_option_contract.side_effect = lambda t, e, s, r: MagicMock(
        symbol=t, lastTradeDateOrContractMonth=e, strike=s, right=r
    )
    client.request_snapshot.side_effect = fake_snapshot
    return client


# ── Chain refresh tests ───────────────────────────────────────────────────────

def test_refresh_option_chains_stores_entries(tmp_db):
    chain_data = {
        "AAPL": [
            ("SMART", "20240119", 180.0, "C"),
            ("SMART", "20240119", 180.0, "P"),
            ("SMART", "20240119", 185.0, "C"),
            ("SMART", "20240119", 185.0, "P"),
        ]
    }
    client = _make_chain_client(chain_data)
    total = refresh_option_chains(client, ["AAPL"])

    assert total == 4

    conn = get_connection()
    rows = conn.execute("SELECT * FROM option_chains WHERE ticker='AAPL'").fetchall()
    conn.close()
    assert len(rows) == 4


def test_refresh_option_chains_upserts(tmp_db):
    """Running refresh twice should not duplicate rows."""
    chain_data = {"AAPL": [("SMART", "20240119", 180.0, "C")]}
    client = _make_chain_client(chain_data)
    refresh_option_chains(client, ["AAPL"])
    refresh_option_chains(client, ["AAPL"])

    conn = get_connection()
    count = conn.execute(
        "SELECT COUNT(*) FROM option_chains WHERE ticker='AAPL'"
    ).fetchone()[0]
    conn.close()
    assert count == 1


def test_refresh_empty_chain(tmp_db):
    """No chain data returned → 0 rows, no crash."""
    client = _make_chain_client({})
    total = refresh_option_chains(client, ["AAPL"])
    assert total == 0


# ── Option quote tests ────────────────────────────────────────────────────────

def _seed_chain(db_path, ticker, expiry, strikes, rights=("C", "P")):
    import sqlite3
    conn = sqlite3.connect(db_path)
    for strike in strikes:
        for right in rights:
            conn.execute(
                "INSERT OR REPLACE INTO option_chains (ticker,expiry,strike,right) VALUES (?,?,?,?)",
                (ticker, expiry, strike, right)
            )
    conn.commit()
    conn.close()


def test_run_option_etl_writes_rows(tmp_db):
    _seed_chain(tmp_db, "AAPL", "20240119", [180.0, 185.0])

    snapshots = {
        ("AAPL", "20240119", 180.0, "C"): {
            "ts": "2024-01-15T14:30:00+00:00",
            "bid": 3.1, "ask": 3.2, "last": 3.15,
            "volume": 500, "open_interest": 1200,
            "implied_vol": 0.28, "delta": 0.55,
            "gamma": 0.04, "theta": -0.05, "vega": 0.12,
        },
        ("AAPL", "20240119", 180.0, "P"): {
            "ts": "2024-01-15T14:30:00+00:00",
            "bid": 1.0, "ask": 1.1, "last": 1.05,
            "volume": 300, "open_interest": 800,
            "implied_vol": 0.30, "delta": -0.45,
            "gamma": 0.04, "theta": -0.04, "vega": 0.11,
        },
        ("AAPL", "20240119", 185.0, "C"): {
            "ts": "2024-01-15T14:30:00+00:00",
            "bid": 1.5, "ask": 1.6, "last": 1.55,
            "volume": 200, "open_interest": 600,
            "implied_vol": 0.25, "delta": 0.35,
            "gamma": 0.03, "theta": -0.03, "vega": 0.09,
        },
        ("AAPL", "20240119", 185.0, "P"): {
            "ts": "2024-01-15T14:30:00+00:00",
            "bid": 3.8, "ask": 3.9, "last": 3.85,
            "volume": 150, "open_interest": 500,
            "implied_vol": 0.27, "delta": -0.65,
            "gamma": 0.03, "theta": -0.04, "vega": 0.10,
        },
    }
    client = _make_quote_client(snapshots)
    rows = run_option_etl(client, ["AAPL"], expiry_cycles=1)

    assert rows == 4

    conn = get_connection()
    db_rows = conn.execute("SELECT * FROM option_quotes").fetchall()
    conn.close()
    assert len(db_rows) == 4


def test_run_option_etl_greeks_stored(tmp_db):
    _seed_chain(tmp_db, "AAPL", "20240119", [180.0], rights=("C",))

    snapshots = {
        ("AAPL", "20240119", 180.0, "C"): {
            "ts": "2024-01-15T14:30:00+00:00",
            "bid": 3.1, "ask": 3.2, "last": 3.15,
            "implied_vol": 0.28, "delta": 0.55,
            "gamma": 0.04, "theta": -0.05, "vega": 0.12,
        }
    }
    client = _make_quote_client(snapshots)
    run_option_etl(client, ["AAPL"], expiry_cycles=1)

    conn = get_connection()
    row = conn.execute("SELECT * FROM option_quotes LIMIT 1").fetchone()
    conn.close()

    assert row["delta"]       == pytest.approx(0.55)
    assert row["implied_vol"] == pytest.approx(0.28)
    assert row["theta"]       == pytest.approx(-0.05)


def test_run_option_etl_no_chain(tmp_db):
    """No chain in DB → 0 rows, logs warning, no crash."""
    client = _make_quote_client({})
    rows = run_option_etl(client, ["AAPL"], expiry_cycles=1)
    assert rows == 0
