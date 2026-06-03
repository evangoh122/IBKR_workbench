"""
config/update_tickers.py
Fetches ALL tickers from US stock exchanges via Finviz,
categorizes them by industry, and updates config/tickers.yaml.
"""
import yaml
import time
import re
import pandas as pd
from typing import Dict, List
from loguru import logger
from finvizfinance.screener.overview import Overview
from pathlib import Path

def fetch_all_finviz_tickers() -> Dict[str, List[str]]:
    """
    Fetches all tickers from Finviz screener and groups them by industry.
    Returns a mapping of industry_name -> list_of_tickers.
    """
    logger.info("Initializing Finviz bulk ticker fetch (this may take 2-4 minutes)...")
    foverview = Overview()
    
    try:
        # Fetch all stocks (limit=-1) with safety sleep to prevent IP blocks
        df = foverview.screener_view(limit=-1, sleep_sec=1, verbose=1)
    except Exception as e:
        logger.error(f"Bulk fetch failed: {e}")
        return {}

    if df.empty or 'Ticker' not in df.columns or 'Industry' not in df.columns:
        logger.error("Failed to retrieve valid columns from Finviz.")
        return {}

    logger.info(f"Retrieved {len(df)} tickers. Categorizing...")
    
    industry_map = {}
    for industry, group in df.groupby('Industry'):
        # Skip N/A or empty industries
        if pd.isna(industry) or str(industry).upper() in ("N/A", ""):
            continue

        # Clean industry name for YAML key
        # 1. Lowercase and replace symbols
        key = str(industry).lower().replace("&", "and").replace("-", " ")
        # 2. Replace non-alphanumeric with space
        key = re.sub(r'[^a-z0-9]', ' ', key)
        # 3. Collapse multiple spaces to single underscore
        key = "_".join(key.split())

        tickers = sorted(list(set(group['Ticker'].tolist())))
        
        # Merge if multiple industries map to same sanitized key
        if key in industry_map:
            industry_map[key] = sorted(list(set(industry_map[key] + tickers)))
        else:
            industry_map[key] = tickers
        
    return industry_map

def update_tickers_yaml(industry_map: Dict[str, List[str]], file_path: str = "config/tickers.yaml"):
    """Overwrites industry groups in tickers.yaml while preserving other keys like options_config."""
    yaml_path = Path(file_path)
    
    if yaml_path.exists():
        with open(yaml_path, 'r') as f:
            full_config = yaml.safe_load(f) or {}
    else:
        full_config = {}

    # Initialize groups if missing
    if "groups" not in full_config:
        full_config["groups"] = {}

    # Update industry groups with fresh data
    for key, tickers in industry_map.items():
        full_config["groups"][key] = {"tickers": tickers}

    with open(yaml_path, 'w') as f:
        yaml.dump(full_config, f, sort_keys=True, indent=2)
    
    logger.info(f"Updated {yaml_path} with {len(industry_map)} industries.")

if __name__ == "__main__":
    industry_map = fetch_all_finviz_tickers()
    if industry_map:
        update_tickers_yaml(industry_map)
    else:
        logger.error("No ticker data fetched. YAML file not updated.")
