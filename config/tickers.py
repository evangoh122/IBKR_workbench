"""
config/tickers.py
Loads ticker lists and per-ticker options config from tickers.yaml.
"""
import os
from pathlib import Path
from typing import Dict, List, Tuple

import yaml

YAML_PATH = os.getenv("TICKERS_YAML", str(Path(__file__).parent / "tickers.yaml"))


def load_config() -> dict:
    with open(YAML_PATH, "r") as f:
        return yaml.safe_load(f)


def get_all_tickers() -> List[str]:
    """Return a flat, deduplicated list of all tickers across all groups."""
    cfg = load_config()
    seen = set()
    tickers = []
    for group in cfg.get("groups", {}).values():
        for t in group.get("tickers", []):
            t = t.strip()
            if t and t not in seen:
                seen.add(t)
                tickers.append(t)
    return tickers


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
