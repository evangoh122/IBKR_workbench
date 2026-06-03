"""
config/update_tickers.py
Fetches ALL tickers from US stock exchanges via Finviz,
categorizes them by industry, and updates config/tickers.yaml.
"""
import yaml
import time
import pandas as pd
from typing import Dict, List
from loguru import logger
from finvizfinance.screener.overview import Overview
from pathlib import Path

def fetch_all_finviz_tickers() -> Dict[str, List[str]]:
    """
    Fetches all tickers from Finviz screener and groups them by industry.
    This replaces the industry-by-industry scraping with a single bulk fetch.
    """
    logger.info("Initializing Finviz bulk ticker fetch...")
    foverview = Overview()
    
    # Fetch all stocks (limit=-1) with a safety sleep between page requests
    # Note: This might take a few minutes as there are 8000+ tickers
    try:
        df = foverview.screener_view(limit=-1, sleep_sec=1, verbose=1)
    except Exception as e:
        logger.error(f"Bulk fetch failed: {e}")
        return {}

    if df.empty or 'Ticker' not in df.columns or 'Industry' not in df.columns:
        logger.error("Failed to retrieve valid data from Finviz.")
        return {}

    logger.info(f"Retrieved {len(df)} tickers. Categorizing by industry...")
    
    # Group by industry
    industry_map = {}
    for industry, group in df.groupby('Industry'):
        # Sanitize industry name for YAML key
        key = str(industry).lower().replace(" ", "_").replace("&", "and").replace("-", "_")
        tickers = sorted(group['Ticker'].tolist())
        industry_map[key] = tickers
        
    return industry_map

def update_tickers_yaml(industry_map: Dict[str, List[str]], file_path: str = "config/tickers.yaml"):
    """Overwrites or updates the tickers.yaml file in the project's expected format."""
    yaml_path = Path(file_path)
    
    if yaml_path.exists():
        with open(yaml_path, 'r') as f:
            full_config = yaml.safe_load(f) or {}
    else:
        full_config = {}

    if "groups" not in full_config:
        full_config["groups"] = {}

    # Update industry groups
    for industry, tickers in industry_map.items():
        # Sanitize industry name for YAML key
        key = str(industry).lower().replace(" ", "_").replace("&", "and").replace("-", "_")
        full_config["groups"][key] = {"tickers": tickers}

    with open(yaml_path, 'w') as f:
        yaml.dump(full_config, f, sort_keys=True)
    
    logger.info(f"Updated {yaml_path} with {len(industry_map)} industries.")

if __name__ == "__main__":
    industry_map = fetch_all_finviz_tickers()
    if industry_map:
        update_tickers_yaml(industry_map)
    else:
        logger.error("No ticker data fetched. YAML file not updated.")
