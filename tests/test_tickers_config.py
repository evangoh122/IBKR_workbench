"""
tests/test_tickers_config.py
Tests for YAML ticker loader.
"""
import os
import textwrap
import pytest
from config.tickers import get_all_tickers, get_all_ticker_symbols, get_tickers_by_group, get_expiry_cycles


SAMPLE_YAML = textwrap.dedent("""
    groups:
      tech:
        description: "Tech stocks"
        tickers:
          - AAPL
          - MSFT
          - NVDA
      etfs:
        description: "ETFs"
        tickers:
          - SPY
          - QQQ

    options_config:
      SPY:
        expiry_cycles: 1
      AAPL:
        expiry_cycles: 3
""")


@pytest.fixture
def yaml_file(tmp_path, monkeypatch):
    f = tmp_path / "tickers.yaml"
    f.write_text(SAMPLE_YAML)
    monkeypatch.setenv("TICKERS_YAML", str(f))
    return f


def test_get_all_tickers_flat(yaml_file):
    tickers = get_all_ticker_symbols()
    assert tickers == ["AAPL", "MSFT", "NVDA", "SPY", "QQQ"]


def test_get_all_tickers_deduplicates(tmp_path, monkeypatch):
    yaml = textwrap.dedent("""
        groups:
          a:
            tickers: [AAPL, MSFT]
          b:
            tickers: [MSFT, TSLA]
    """)
    f = tmp_path / "tickers.yaml"
    f.write_text(yaml)
    monkeypatch.setenv("TICKERS_YAML", str(f))

    tickers = get_all_ticker_symbols()
    assert tickers.count("MSFT") == 1
    assert len(tickers) == 3


def test_get_tickers_by_group(yaml_file):
    groups = get_tickers_by_group()
    assert "tech" in groups
    assert "etfs" in groups
    assert "AAPL" in groups["tech"]
    assert "SPY"  in groups["etfs"]


def test_get_expiry_cycles_override(yaml_file):
    assert get_expiry_cycles("SPY")  == 1
    assert get_expiry_cycles("AAPL") == 3


def test_get_expiry_cycles_default(yaml_file):
    assert get_expiry_cycles("MSFT")        == 2   # not in options_config
    assert get_expiry_cycles("MSFT", default=4) == 4


def test_get_all_tickers_empty(tmp_path, monkeypatch):
    f = tmp_path / "tickers.yaml"
    f.write_text("groups: {}")
    monkeypatch.setenv("TICKERS_YAML", str(f))
    assert get_all_tickers() == []
