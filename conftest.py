import sys
from types import ModuleType
from unittest.mock import MagicMock


def _make_ibapi_stubs():
    """
    ibapi is not pip-installable (downloaded from IBKR's website).
    Provide minimal stubs so imports resolve at collection time.
    Tests mock the client entirely so no real ibapi behaviour is needed.
    EWrapper/EClient must be real classes — MagicMock bases cause a
    metaclass conflict when IBKRClient inherits from them.
    """
    class EWrapper:
        pass

    class EClient:
        def __init__(self, wrapper=None):
            pass

    contract_mod = ModuleType("ibapi.contract")
    contract_mod.Contract = type("Contract", (), {})

    ticktype_mod = ModuleType("ibapi.ticktype")
    ticktype_mod.TickTypeEnum = type("TickTypeEnum", (), {})

    wrapper_mod = ModuleType("ibapi.wrapper")
    wrapper_mod.EWrapper = EWrapper

    client_mod = ModuleType("ibapi.client")
    client_mod.EClient = EClient

    root_mod = ModuleType("ibapi")

    for name, mod in [
        ("ibapi", root_mod),
        ("ibapi.client", client_mod),
        ("ibapi.wrapper", wrapper_mod),
        ("ibapi.contract", contract_mod),
        ("ibapi.ticktype", ticktype_mod),
    ]:
        sys.modules.setdefault(name, mod)


_make_ibapi_stubs()

import pytest
import db.database as db_module


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    """Isolated SQLite DB for each test — patches DB_PATH everywhere."""
    db_file = str(tmp_path / "test_ibkr.db")
    monkeypatch.setenv("DB_PATH", db_file)
    monkeypatch.setattr(db_module, "DB_PATH", db_file)

    db_module.init_db()
    yield db_file


@pytest.fixture
def db_conn(tmp_db):
    """Open connection to the temp DB, closed after the test."""
    conn = db_module.get_connection()
    yield conn
    conn.close()


@pytest.fixture
def sample_tickers():
    return ["AAPL", "MSFT", "SPY"]
