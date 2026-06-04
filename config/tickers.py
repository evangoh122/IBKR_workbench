"""
config/tickers.py
Loads ticker lists and per-ticker options config from tickers.yaml.
"""
import os
from pathlib import Path
from typing import Dict, List

import yaml

_DEFAULT_YAML = str(Path(__file__).parent / "tickers.yaml")


def load_config() -> dict:
    path = os.getenv("TICKERS_YAML", _DEFAULT_YAML)
    if not Path(path).exists():
        from loguru import logger
        logger.warning(
            f"Tickers config not found at '{path}' — returning empty config. "
            f"Set TICKERS_YAML in .env or create the file."
        )
        return {"groups": {}}
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_all_tickers() -> List[dict]:
    """Return a flat list of dictionaries with full contract details."""
    cfg = load_config()
    seen = set()
    tickers = []
    for group_name, group in cfg.get("groups", {}).items():
        secType  = group.get("secType", "STK")
        exchange = group.get("exchange", "SMART")
        currency = group.get("currency", "USD")

        for t in group.get("tickers", []):
            t = str(t).strip()
            if t and t not in seen:
                seen.add(t)
                tickers.append({
                    "symbol": t,
                    "secType": secType,
                    "exchange": exchange,
                    "currency": currency
                })
    return tickers

def get_tickers_by_groups(group_names: List[str]) -> List[dict]:
    """Return tickers from specified group names only."""
    cfg = load_config()
    seen = set()
    tickers = []
    for group_name in group_names:
        group = cfg.get("groups", {}).get(group_name, {})
        secType  = group.get("secType", "STK")
        exchange = group.get("exchange", "SMART")
        currency = group.get("currency", "USD")
        for t in group.get("tickers", []):
            t = str(t).strip()
            if t and t not in seen:
                seen.add(t)
                tickers.append({
                    "symbol": t,
                    "secType": secType,
                    "exchange": exchange,
                    "currency": currency
                })
    return tickers


def get_all_ticker_symbols() -> List[str]:
    """Return just the flat list of string symbols (legacy)."""
    return [t["symbol"] for t in get_all_tickers()]


def get_tickers_by_group() -> Dict[str, List[str]]:
    """Return tickers organised by group name."""
    cfg = load_config()
    return {
        name: group.get("tickers", [])
        for name, group in cfg.get("groups", {}).items()
    }


def get_expiry_cycles(ticker: str, default: int = 2) -> int:
    """Return the options expiry_cycles override for a ticker, or the default."""
    cfg = load_config()
    return (cfg
            .get("options_config", {})
            .get(ticker, {})
            .get("expiry_cycles", default))


if __name__ == "__main__":
    tickers = get_all_tickers()
    print(f"Total tickers: {len(tickers)}")
    for group, ts in get_tickers_by_group().items():
        print(f"  {group}: {', '.join(ts)}")
